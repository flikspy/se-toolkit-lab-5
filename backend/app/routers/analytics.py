"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


async def _get_lab_and_task_ids(session: AsyncSession, lab: str) -> tuple[int, list[int]]:
    """Find the lab item by title and return its ID and child task IDs.

    Args:
        session: Database session
        lab: Lab identifier (e.g., "lab-04")

    Returns:
        Tuple of (lab_id, list of task_ids)
    """
    # Convert "lab-04" to "Lab 04" for title matching
    lab_title = lab.replace("lab-", "Lab ").replace("LAB-", "Lab ")

    # Find the lab
    lab_stmt = select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.one_or_none()

    if not lab_item:
        return 0, []

    # Extract the model instance from the row (SQLModel returns Row objects)
    if hasattr(lab_item, "ItemRecord"):
        lab_item = lab_item.ItemRecord
    elif hasattr(lab_item, "_mapping"):
        lab_item = lab_item[0]

    # Find all tasks that belong to this lab
    tasks_stmt = select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    tasks_result = await session.exec(tasks_stmt)
    task_items = tasks_result.all()

    # Extract IDs from task items (may be Row objects or model instances)
    task_ids = []
    for task in task_items:
        if hasattr(task, "ItemRecord"):
            task_ids.append(task.ItemRecord.id)
        elif hasattr(task, "_mapping"):
            task_ids.append(task[0].id)
        else:
            task_ids.append(task.id)

    return lab_item.id, task_ids


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.
    
    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    # Build bucket expression using CASE WHEN
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    ).label("bucket")
    
    # Query interactions for tasks in this lab with scores
    stmt = (
        select(bucket_expr, func.count().label("count"))
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by("bucket")
    )
    
    result = await session.exec(stmt)
    rows = result.all()
    
    # Build result dict from query
    bucket_counts = {row.bucket: row.count for row in rows}
    
    # Always return all four buckets
    return [
        {"bucket": "0-25", "count": bucket_counts.get("0-25", 0)},
        {"bucket": "26-50", "count": bucket_counts.get("26-50", 0)},
        {"bucket": "51-75", "count": bucket_counts.get("51-75", 0)},
        {"bucket": "76-100", "count": bucket_counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    _, task_ids = await _get_lab_and_task_ids(session, lab)

    if not task_ids:
        return []

    # Get task titles
    tasks_stmt = select(ItemRecord).where(ItemRecord.id.in_(task_ids)).order_by(ItemRecord.title)
    tasks_result = await session.exec(tasks_stmt)
    tasks = {}
    for task in tasks_result.all():
        # Handle Row objects vs model instances
        if hasattr(task, "ItemRecord"):
            tasks[task.ItemRecord.id] = task.ItemRecord.title
        elif hasattr(task, "_mapping"):
            tasks[task[0].id] = task[0].title
        else:
            tasks[task.id] = task.title

    # Query interactions grouped by task
    stmt = (
        select(InteractionLog.item_id, func.avg(InteractionLog.score).label("avg_score"), func.count().label("attempts"))
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(InteractionLog.item_id)
    )

    result = await session.exec(stmt)
    rows = result.all()

    # Build response
    response = []
    for task_id, avg_score, attempts in rows:
        response.append({
            "task": tasks[task_id],
            "avg_score": round(avg_score, 1),
            "attempts": attempts,
        })

    # Sort by task title
    response.sort(key=lambda x: x["task"])

    return response


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.
    
    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    if not task_ids:
        return []
    
    # Query interactions grouped by date
    stmt = (
        select(func.date(InteractionLog.created_at).label("date"), func.count().label("submissions"))
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by("date")
        .order_by("date")
    )
    
    result = await session.exec(stmt)
    rows = result.all()
    
    return [{"date": str(row.date), "submissions": row.submissions} for row in rows]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.
    
    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    _, task_ids = await _get_lab_and_task_ids(session, lab)
    
    if not task_ids:
        return []
    
    # Query interactions joined with learners, grouped by student_group
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(InteractionLog.learner_id)).label("students"),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    
    result = await session.exec(stmt)
    rows = result.all()
    
    return [
        {
            "group": row.group,
            "avg_score": round(row.avg_score, 1),
            "students": row.students,
        }
        for row in rows
    ]
