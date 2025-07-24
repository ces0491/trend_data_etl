# src/api/routes/data_quality.py
"""
Data quality monitoring and reporting endpoints
Fixed version with proper type handling and imports
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, List, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_

from api.dependencies import get_db_session, get_pagination_params, PaginationParams
from api.models import (
    QualitySummaryResponse, QualityDetailResponse, ProcessingLogResponse,
    PaginatedResponse, PaginationResponse, ProcessingStatus
)
from database.models import QualityScore, DataProcessingLog, Platform, StreamingRecord

router = APIRouter()


@router.get("/summary", response_model=QualitySummaryResponse)
async def get_quality_summary(
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    quality_threshold: float = Query(90.0, ge=0, le=100, description="Quality threshold for reporting"),
    session: Session = Depends(get_db_session)
):
    """
    Get overall data quality summary
    
    Returns high-level data quality metrics including average scores,
    files processed, and platform-level quality statistics.
    """
    # Calculate date threshold
    date_threshold = datetime.utcnow() - timedelta(days=days)
    
    # Get quality scores from the specified period
    quality_query = session.query(QualityScore).filter(
        QualityScore.measured_at >= date_threshold
    )
    
    quality_records = quality_query.all()
    
    if not quality_records:
        return QualitySummaryResponse(
            total_files_processed=0,
            average_quality_score=0.0,
            files_above_threshold=0,
            quality_threshold=quality_threshold,
            platforms_analyzed=0,
            total_records_processed=0,
            last_updated=datetime.utcnow()
        )
    
    # Calculate summary statistics
    total_files = len(quality_records)
    total_score = sum(float(q.overall_score) for q in quality_records)
    average_score = total_score / total_files
    files_above_threshold = sum(1 for q in quality_records if float(q.overall_score) >= quality_threshold)
    
    # Get platform count
    platforms_analyzed = session.query(QualityScore.platform_id).filter(
        QualityScore.measured_at >= date_threshold
    ).distinct().count()
    
    # Get total records processed from processing logs - Fixed type handling
    total_records_result = session.query(
        func.sum(DataProcessingLog.records_processed)
    ).filter(
        DataProcessingLog.started_at >= date_threshold
    ).scalar()
    
    total_records = int(total_records_result) if total_records_result is not None else 0
    
    return QualitySummaryResponse(
        total_files_processed=total_files,
        average_quality_score=round(average_score, 2),
        files_above_threshold=files_above_threshold,
        quality_threshold=quality_threshold,
        platforms_analyzed=platforms_analyzed,
        total_records_processed=total_records,
        last_updated=datetime.utcnow()
    )


@router.get("/details", response_model=List[QualityDetailResponse])
async def get_quality_details(
    platform: Optional[str] = Query(None, description="Filter by platform code"),
    min_score: Optional[float] = Query(None, ge=0, le=100, description="Minimum quality score filter"),
    max_score: Optional[float] = Query(None, ge=0, le=100, description="Maximum quality score filter"),
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    session: Session = Depends(get_db_session)
):
    """
    Get detailed quality information for files
    
    Returns detailed quality scores and validation results for individual
    files, with filtering options by platform and score ranges.
    """
    # Calculate date threshold
    date_threshold = datetime.utcnow() - timedelta(days=days)
    
    # Build query
    query = session.query(QualityScore).join(Platform).filter(
        QualityScore.measured_at >= date_threshold
    )
    
    # Apply filters
    if platform:
        query = query.filter(Platform.code == platform)
    
    if min_score is not None:
        query = query.filter(QualityScore.overall_score >= min_score)
    
    if max_score is not None:
        query = query.filter(QualityScore.overall_score <= max_score)
    
    # Execute query
    quality_records = query.order_by(desc(QualityScore.measured_at)).limit(limit).all()
    
    return [
        QualityDetailResponse(
            id=q.id,
            platform_code=q.platform.code,
            platform_name=q.platform.name,
            file_path=q.file_path,
            overall_score=float(q.overall_score),
            completeness_score=float(q.completeness_score) if q.completeness_score else None,
            consistency_score=float(q.consistency_score) if q.consistency_score else None,
            validity_score=float(q.validity_score) if q.validity_score else None,
            accuracy_score=float(q.accuracy_score) if q.accuracy_score else None,
            issues_found=q.quality_details.get('issues', []) if q.quality_details else None,
            recommendations=q.recommendations,
            measured_at=q.measured_at
        ) for q in quality_records
    ]


@router.get("/trends")
async def get_quality_trends(
    platform: Optional[str] = Query(None, description="Filter by platform code"),
    days: int = Query(90, ge=7, le=365, description="Number of days for trend analysis"),
    aggregation: str = Query("daily", regex="^(daily|weekly|monthly)$", description="Aggregation period"),
    session: Session = Depends(get_db_session)
):
    """
    Get quality trends over time
    
    Returns time-series data showing how data quality has changed
    over the specified period, aggregated by day, week, or month.
    """
    # Calculate date threshold
    date_threshold = datetime.utcnow() - timedelta(days=days)
    
    # Build base query
    query = session.query(QualityScore).join(Platform).filter(
        QualityScore.measured_at >= date_threshold
    )
    
    if platform:
        query = query.filter(Platform.code == platform)
    
    # Build aggregation query based on period
    if aggregation == "daily":
        date_trunc = func.date(QualityScore.measured_at)
    elif aggregation == "weekly":
        date_trunc = func.date_trunc('week', QualityScore.measured_at)
    else:  # monthly
        date_trunc = func.date_trunc('month', QualityScore.measured_at)
    
    # Get trend data
    trend_data = query.with_entities(
        date_trunc.label('period'),
        Platform.code.label('platform_code'),
        func.avg(QualityScore.overall_score).label('avg_score'),
        func.min(QualityScore.overall_score).label('min_score'),
        func.max(QualityScore.overall_score).label('max_score'),
        func.count(QualityScore.id).label('file_count')
    ).group_by(date_trunc, Platform.code).order_by(date_trunc).all()
    
    trends = []
    for trend in trend_data:
        trends.append({
            "period": trend.period.isoformat() if trend.period else None,
            "platform_code": trend.platform_code,
            "average_score": float(trend.avg_score) if trend.avg_score else 0.0,
            "min_score": float(trend.min_score) if trend.min_score else 0.0,
            "max_score": float(trend.max_score) if trend.max_score else 0.0,
            "file_count": trend.file_count or 0
        })
    
    return {
        "analysis_period": {
            "days": days,
            "aggregation": aggregation,
            "from_date": date_threshold.date().isoformat(),
            "to_date": datetime.utcnow().date().isoformat()
        },
        "filters_applied": {
            "platform": platform
        },
        "trend_data": trends,
        "data_points": len(trends)
    }


@router.get("/platform/{platform_code}")
async def get_platform_quality(
    platform_code: str,
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    session: Session = Depends(get_db_session)
):
    """
    Get quality metrics for a specific platform
    
    Returns detailed quality analysis for a single platform including
    score distribution, common issues, and improvement recommendations.
    """
    # Verify platform exists
    platform = session.query(Platform).filter(Platform.code == platform_code).first()
    if not platform:
        raise HTTPException(
            status_code=404, 
            detail=f"Platform '{platform_code}' not found"
        )
    
    # Calculate date threshold
    date_threshold = datetime.utcnow() - timedelta(days=days)
    
    # Get quality records for this platform
    quality_records = session.query(QualityScore).filter(
        and_(
            QualityScore.platform_id == platform.id,
            QualityScore.measured_at >= date_threshold
        )
    ).all()
    
    if not quality_records:
        return {
            "platform_code": platform_code,
            "platform_name": platform.name,
            "analysis_period": {
                "days": days,
                "from_date": date_threshold.date().isoformat(),
                "to_date": datetime.utcnow().date().isoformat()
            },
            "quality_summary": {
                "files_processed": 0,
                "average_score": 0.0,
                "min_score": 0.0,
                "max_score": 0.0
            },
            "score_distribution": {},
            "common_issues": [],
            "recommendations": []
        }
    
    # Calculate statistics
    scores = [float(q.overall_score) for q in quality_records]
    avg_score = sum(scores) / len(scores)
    min_score = min(scores)
    max_score = max(scores)
    
    # Score distribution (buckets)
    score_buckets = {
        "excellent": sum(1 for s in scores if s >= 95),
        "good": sum(1 for s in scores if 85 <= s < 95),
        "fair": sum(1 for s in scores if 70 <= s < 85),
        "poor": sum(1 for s in scores if s < 70)
    }
    
    # Extract common issues
    all_issues = []
    for record in quality_records:
        if record.quality_details and 'issues' in record.quality_details:
            all_issues.extend(record.quality_details['issues'])
    
    # Count issue types
    issue_counts = {}
    for issue in all_issues:
        if isinstance(issue, dict) and 'rule_name' in issue:
            rule_name = issue['rule_name']
            issue_counts[rule_name] = issue_counts.get(rule_name, 0) + 1
    
    # Get top 5 issues
    common_issues = [
        {"issue_type": issue, "count": count}
        for issue, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    ]
    
    # Generate recommendations
    recommendations = []
    if avg_score < 70:
        recommendations.append("Platform requires immediate attention - quality scores are critically low")
    elif avg_score < 85:
        recommendations.append("Platform quality is below target - review data processing pipeline")
    
    if score_buckets["poor"] > len(scores) * 0.2:  # More than 20% poor quality
        recommendations.append("High number of poor quality files - investigate file format issues")
    
    if "missing_required_columns" in issue_counts:
        recommendations.append("File format validation needed - missing required columns detected")
    
    if "invalid_date_format" in issue_counts:
        recommendations.append("Date parsing improvements needed - inconsistent date formats detected")
    
    return {
        "platform_code": platform_code,
        "platform_name": platform.name,
        "analysis_period": {
            "days": days,
            "from_date": date_threshold.date().isoformat(),
            "to_date": datetime.utcnow().date().isoformat()
        },
        "quality_summary": {
            "files_processed": len(quality_records),
            "average_score": round(avg_score, 2),
            "min_score": round(min_score, 2),
            "max_score": round(max_score, 2)
        },
        "score_distribution": score_buckets,
        "common_issues": common_issues,
        "recommendations": recommendations
    }


@router.get("/processing-logs", response_model=List[ProcessingLogResponse])
async def get_processing_logs(
    platform: Optional[str] = Query(None, description="Filter by platform code"),
    status: Optional[str] = Query(None, description="Filter by processing status"),
    days: int = Query(7, ge=1, le=90, description="Number of days to look back"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    session: Session = Depends(get_db_session)
):
    """
    Get file processing logs
    
    Returns detailed logs of file processing activities including
    success/failure status, processing times, and error messages.
    """
    # Calculate date threshold
    date_threshold = datetime.utcnow() - timedelta(days=days)
    
    # Build query
    query = session.query(DataProcessingLog).join(Platform).filter(
        DataProcessingLog.started_at >= date_threshold
    )
    
    # Apply filters
    if platform:
        query = query.filter(Platform.code == platform)
    
    if status:
        query = query.filter(DataProcessingLog.processing_status == status)
    
    # Execute query
    logs = query.order_by(desc(DataProcessingLog.started_at)).limit(limit).all()
    
    return [
        ProcessingLogResponse(
            id=log.id,
            file_name=log.file_name,
            file_path=log.file_path,
            file_size=log.file_size,
            platform_code=log.platform.code,
            platform_name=log.platform.name,
            processing_status=ProcessingStatus(log.processing_status),  # Fixed enum usage
            records_processed=log.records_processed,
            records_failed=log.records_failed,
            records_skipped=log.records_skipped,
            quality_score=float(log.quality_score) if log.quality_score else None,
            error_message=log.error_message,
            started_at=log.started_at,
            completed_at=log.completed_at,
            processing_duration_ms=log.processing_duration_ms
        ) for log in logs
    ]


@router.get("/issues")
async def get_quality_issues(
    platform: Optional[str] = Query(None, description="Filter by platform code"),
    severity: Optional[str] = Query(None, description="Filter by issue severity"),
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of issues"),
    session: Session = Depends(get_db_session)
):
    """
    Get detailed quality issues and validation problems
    
    Returns specific validation issues found in data files with
    context and recommendations for resolution.
    """
    # Calculate date threshold
    date_threshold = datetime.utcnow() - timedelta(days=days)
    
    # Build query
    query = session.query(QualityScore).join(Platform).filter(
        QualityScore.measured_at >= date_threshold
    )
    
    if platform:
        query = query.filter(Platform.code == platform)
    
    quality_records = query.order_by(desc(QualityScore.measured_at)).all()
    
    # Extract and categorize issues
    all_issues = []
    for record in quality_records:
        if record.quality_details and 'issues' in record.quality_details:
            for issue in record.quality_details['issues']:
                if isinstance(issue, dict):
                    # Add context information
                    enhanced_issue = {
                        **issue,
                        "platform_code": record.platform.code,
                        "platform_name": record.platform.name,
                        "file_path": record.file_path,
                        "file_quality_score": float(record.overall_score),
                        "measured_at": record.measured_at.isoformat()
                    }
                    all_issues.append(enhanced_issue)
    
    # Apply severity filter
    if severity:
        all_issues = [issue for issue in all_issues if issue.get('severity') == severity]
    
    # Sort by severity and limit results
    severity_order = {'critical': 0, 'error': 1, 'warning': 2, 'info': 3}
    all_issues.sort(key=lambda x: (severity_order.get(x.get('severity', 'info'), 3), x.get('measured_at', '')), reverse=True)
    
    return {
        "analysis_period": {
            "days": days,
            "from_date": date_threshold.date().isoformat(),
            "to_date": datetime.utcnow().date().isoformat()
        },
        "filters_applied": {
            "platform": platform,
            "severity": severity
        },
        "total_issues_found": len(all_issues),
        "issues_shown": min(len(all_issues), limit),
        "issues": all_issues[:limit]
    }


@router.get("/report")
async def generate_quality_report(
    platform: Optional[str] = Query(None, description="Generate report for specific platform"),
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    session: Session = Depends(get_db_session)
):
    """
    Generate comprehensive data quality report
    
    Returns a detailed report suitable for management review including
    executive summary, key metrics, trends, and recommendations.
    """
    # Calculate date threshold
    date_threshold = datetime.utcnow() - timedelta(days=days)
    
    # Build base query
    quality_query = session.query(QualityScore).join(Platform).filter(
        QualityScore.measured_at >= date_threshold
    )
    
    if platform:
        quality_query = quality_query.filter(Platform.code == platform)
    
    quality_records = quality_query.all()
    
    if not quality_records:
        return {
            "report_metadata": {
                "generated_at": datetime.utcnow().isoformat(),
                "analysis_period": f"{days} days",
                "platform_filter": platform
            },
            "executive_summary": {
                "status": "No data available for the specified period"
            }
        }
    
    # Calculate key metrics
    scores = [float(q.overall_score) for q in quality_records]
    avg_score = sum(scores) / len(scores)
    files_processed = len(quality_records)
    
    # Platform breakdown
    platform_scores = {}
    for record in quality_records:
        platform_code = record.platform.code
        if platform_code not in platform_scores:
            platform_scores[platform_code] = []
        platform_scores[platform_code].append(float(record.overall_score))
    
    platform_summary = {}
    for plt_code, plt_scores in platform_scores.items():
        platform_summary[plt_code] = {
            "files_processed": len(plt_scores),
            "average_score": sum(plt_scores) / len(plt_scores),
            "min_score": min(plt_scores),
            "max_score": max(plt_scores)
        }
    
    # Determine overall status
    if avg_score >= 95:
        status = "EXCELLENT"
        status_message = "Data quality exceeds targets across all metrics"
    elif avg_score >= 85:
        status = "GOOD"
        status_message = "Data quality meets targets with minor improvement opportunities"
    elif avg_score >= 70:
        status = "FAIR"
        status_message = "Data quality is acceptable but requires attention to reach targets"
    else:
        status = "POOR"
        status_message = "Data quality is below acceptable levels and requires immediate action"
    
    # Generate recommendations
    recommendations = []
    if avg_score < 90:
        recommendations.append("Implement automated data validation checks in the ingestion pipeline")
    
    low_quality_platforms = [plt for plt, data in platform_summary.items() if data['average_score'] < 80]
    if low_quality_platforms:
        recommendations.append(f"Focus improvement efforts on platforms: {', '.join(low_quality_platforms)}")
    
    if files_processed < 10:
        recommendations.append("Increase monitoring frequency to get more comprehensive quality insights")
    
    return {
        "report_metadata": {
            "generated_at": datetime.utcnow().isoformat(),
            "analysis_period": f"{days} days ({date_threshold.date().isoformat()} to {datetime.utcnow().date().isoformat()})",
            "platform_filter": platform or "All platforms",
            "report_version": "1.0"
        },
        "executive_summary": {
            "status": status,
            "message": status_message,
            "overall_quality_score": round(avg_score, 1),
            "files_processed": files_processed,
            "platforms_analyzed": len(platform_summary)
        },
        "key_metrics": {
            "average_quality_score": round(avg_score, 2),
            "files_above_90_percent": sum(1 for s in scores if s >= 90),
            "files_below_70_percent": sum(1 for s in scores if s < 70),
            "score_range": {
                "minimum": round(min(scores), 1),
                "maximum": round(max(scores), 1)
            }
        },
        "platform_breakdown": platform_summary,
        "recommendations": recommendations,
        "next_review_date": (datetime.utcnow() + timedelta(days=7)).date().isoformat()
    }