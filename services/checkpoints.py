from services.db import execute, fetch_one

def update_checkpoint(source: str):
    execute(
        """
        INSERT INTO checkpoints (source, last_run)
        VALUES (%s, NOW())
        ON CONFLICT (source)
        DO UPDATE SET last_run = EXCLUDED.last_run
        """,
        (source,),
    )

def get_checkpoint(source: str):
    row = fetch_one(
        "SELECT last_run FROM checkpoints WHERE source = %s",
        (source,),
    )
    # FIX: Access by column name 'last_run', not index [0]
    return row['last_run'] if row else None