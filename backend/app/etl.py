"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    all_logs: list[dict] = []
    current_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            logs = data["logs"]
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Use the submitted_at of the last log as the new "since" value
            current_since = datetime.fromisoformat(logs[-1]["submitted_at"])

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(
    items: list[dict], session: AsyncSession
) -> tuple[int, dict[tuple[str, str | None], int]]:
    """Load items (labs and tasks) into the database.

    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return tuple of (new_count, item_id_lookup) where item_id_lookup maps
      (lab_short_id, task_short_id) to the database item id
    """
    from app.models.item import ItemRecord

    new_count = 0
    lab_lookup: dict[str, ItemRecord] = {}
    item_id_lookup: dict[tuple[str, str | None], int] = {}

    # Process labs first
    for item in items:
        if item["type"] != "lab":
            continue

        # Check if lab already exists by title
        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab", ItemRecord.title == item["title"]
        )
        result = await session.exec(stmt)
        existing = result.one_or_none()

        if existing is None:
            new_lab = ItemRecord(type="lab", title=item["title"])
            session.add(new_lab)
            await session.flush()  # Get the ID
            existing = new_lab
            new_count += 1

        # Map short lab ID (e.g., "lab-01") to the record
        lab_lookup[item["lab"]] = existing
        # Also add to item_id_lookup for labs (task=None)
        item_id_lookup[(item["lab"], None)] = existing.id

    # Process tasks
    for item in items:
        if item["type"] != "task":
            continue

        # Find parent lab using the short ID
        parent_lab = lab_lookup.get(item["lab"])
        if parent_lab is None:
            continue  # Skip if parent lab not found

        # Check if task already exists
        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == item["title"],
            ItemRecord.parent_id == parent_lab.id,
        )
        result = await session.exec(stmt)
        existing = result.one_or_none()

        if existing is None:
            new_task = ItemRecord(
                type="task", title=item["title"], parent_id=parent_lab.id
            )
            session.add(new_task)
            await session.flush()
            existing = new_task
            new_count += 1

        # Map (lab_short_id, task_short_id) to item id
        item_id_lookup[(item["lab"], item["task"])] = existing.id

    await session.commit()
    return new_count, item_id_lookup


async def load_logs(
    logs: list[dict],
    item_id_lookup: dict[tuple[str, str | None], int],
    session: AsyncSession,
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        item_id_lookup: Dict mapping (lab_short_id, task_short_id) to item id
            from load_items().
        session: Database session.

    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use item_id_lookup to get the item id for (log["lab"], log["task"])
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    from app.models.interaction import InteractionLog
    from app.models.learner import Learner

    new_count = 0

    for log in logs:
        # 1. Find or create learner by external_id
        stmt = select(Learner).where(Learner.external_id == log["student_id"])
        result = await session.exec(stmt)
        learner = result.one_or_none()

        if learner is None:
            learner = Learner(
                external_id=log["student_id"], student_group=log.get("group", "")
            )
            session.add(learner)
            await session.flush()

        # 2. Find matching item using the lookup
        task_key = (log["lab"], log.get("task"))
        item_id = item_id_lookup.get(task_key)

        if item_id is None:
            continue  # Skip if no matching item found

        # 3. Check if interaction already exists (idempotent upsert)
        existing = await session.get(InteractionLog, log["id"])
        if existing is not None:
            continue  # Skip if already exists

        # 4. Create new InteractionLog
        new_interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item_id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(log["submitted_at"]),
        )
        session.add(new_interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict[str, int]:
    """Run the full ETL pipeline.

    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the item_id_lookup to load_logs so it can map short IDs
        to item ids
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    from app.models.interaction import InteractionLog

    # Step 1: Fetch and load items
    raw_items = await fetch_items()
    _, item_id_lookup = await load_items(raw_items, session)

    # Step 2: Determine the last synced timestamp
    stmt = select(InteractionLog).order_by(InteractionLog.created_at.desc()).limit(1)
    result = await session.exec(stmt)
    last_record = result.one_or_none()
    since = last_record.created_at if last_record else None

    # Step 3: Fetch and load logs
    raw_logs = await fetch_logs(since=since)
    new_records = await load_logs(raw_logs, item_id_lookup, session)

    # Get total count
    total_stmt = select(InteractionLog)
    total_result = await session.exec(total_stmt)
    total_records = len(total_result.all())

    return {"new_records": new_records, "total_records": total_records}
