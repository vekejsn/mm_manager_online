from fastapi import APIRouter
import utilites.sqlite3 as sqlite3
from datetime import datetime, timedelta

router = APIRouter()

db_location = './mm_manager/mm_manager.db'


@router.get("/terminals/active")
async def get_terminals_active(hours: int = 24):
    """Returns a list of active terminals.
    Active terminals are those which have communicated with the Manager in the last two days.
    """
    db = sqlite3.db_connection(db_location)
    db.connect()
    # Dates are stored in the format YYYYMMDD in the database.
    start_date = int((datetime.now() - timedelta(hours=hours)).strftime("%Y%m%d"))
    query_str = """SELECT TERMINAL_ID, RECEIVED_DATE, RECEIVED_TIME, MAX(RECEIVED_DATE || RECEIVED_TIME) AS LAST_COMMUNICATION
    FROM (
            SELECT TERMINAL_ID, RECEIVED_DATE, RECEIVED_TIME
            FROM TALARM
            WHERE CAST(RECEIVED_DATE AS INTEGER) >=  {}
            UNION ALL
            SELECT TERMINAL_ID, RECEIVED_DATE, RECEIVED_TIME
            FROM TAUTH
            WHERE CAST(RECEIVED_DATE AS INTEGER) >=  {}
            UNION ALL
            SELECT TERMINAL_ID, RECEIVED_DATE, RECEIVED_TIME
            FROM TCALLST
            WHERE CAST(RECEIVED_DATE AS INTEGER) >=  {}
            UNION ALL
            SELECT TERMINAL_ID, RECEIVED_DATE, RECEIVED_TIME
            FROM TCASHST
            WHERE CAST(RECEIVED_DATE AS INTEGER) >=  {}
            UNION ALL
            SELECT TERMINAL_ID, RECEIVED_DATE, RECEIVED_TIME
            FROM TCDR
            WHERE CAST(RECEIVED_DATE AS INTEGER) >=  {}
            UNION ALL
            SELECT TERMINAL_ID, RECEIVED_DATE, RECEIVED_TIME
            FROM TCOLLST
            WHERE CAST(RECEIVED_DATE AS INTEGER) >=  {}
            UNION ALL
            SELECT TERMINAL_ID, RECEIVED_DATE, RECEIVED_TIME
            FROM TOPCODE
            WHERE CAST(RECEIVED_DATE AS INTEGER) >=  {}
            UNION ALL
            SELECT TERMINAL_ID, RECEIVED_DATE, RECEIVED_TIME
            FROM TPERFST
            WHERE CAST(RECEIVED_DATE AS INTEGER) >=  {}
            UNION ALL
            SELECT TERMINAL_ID, RECEIVED_DATE, RECEIVED_TIME
            FROM TSTATUS
            WHERE CAST(RECEIVED_DATE AS INTEGER) >=  {}
        ) AS T
    GROUP BY TERMINAL_ID
    ORDER BY LAST_COMMUNICATION DESC
        """.format(*[start_date]*9)
    data = db.execute(query_str)
    # Drop all LAST_COMMUNICATION columns
    db.close()
    return {
        "data": data[::-1] if data is not None else [],
        "count": len(data or [])
    }


@router.get("/terminals/alarms")
async def get_terminals_alarms(page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TALARM"
    count_str = "SELECT COUNT(*) FROM TALARM"
    if start_date is not None:
        data_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
        count_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
        count_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/alarms/{alarm_id}")
async def get_terminals_alarms(alarm_id: int, page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TALARM WHERE ID = {}".format(alarm_id)
    count_str = "SELECT COUNT(*) FROM TALARM WHERE ID = {}".format(alarm_id)
    if start_date is not None:
        data_str += " AND RECEIVED_DATE >= '{}'".format(start_date)
        count_str += " AND RECEIVED_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND RECEIVED_DATE <= '{}'".format(end_date)
        count_str += " AND RECEIVED_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(terminal_id)
    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/auths")
async def get_terminals_auths(page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TAUTH"
    count_str = "SELECT COUNT(*) FROM TAUTH"
    if start_date is not None:
        data_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
        count_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
        count_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)

    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)

    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/call_statistics")
async def get_terminals_call_statistics(page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TCALLST"
    count_str = "SELECT COUNT(*) FROM TCALLST"
    if start_date is not None:
        data_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
        count_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
        count_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/cash_statistics")
async def get_terminals_cash_statistics(page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TCASHST"
    count_str = "SELECT COUNT(*) FROM TCASHST"
    if start_date is not None:
        data_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
        count_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
        count_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/call_detail_records")
async def get_terminals_cdr(page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TCDR"
    count_str = "SELECT COUNT(*) FROM TCDR"
    if start_date is not None:
        data_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
        count_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
        count_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/collection_statistics")
async def get_terminals_collection_statistics(page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TCOLLST"
    count_str = "SELECT COUNT(*) FROM TCOLLST"
    if start_date is not None:
        data_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
        count_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
        count_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/topcode")
async def get_terminals_topcode(page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TOPCODE"
    count_str = "SELECT COUNT(*) FROM TOPCODE"
    if start_date is not None:
        data_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
        count_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
        count_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/performance_statistics")
async def get_terminals_peformance_statistics(page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TPERFST"
    count_str = "SELECT COUNT(*) FROM TPERFST"
    if start_date is not None:
        data_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
        count_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
        count_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/statuses")
async def get_terminals_statuses(page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TSTATUS"
    count_str = "SELECT COUNT(*) FROM TSTATUS"
    if start_date is not None:
        data_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
        count_str += " WHERE RECEIVED_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
        count_str += " AND RECEIVED_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE RECEIVED_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/software_versions")
async def get_terminals_software_versions(page: int = 1, limit: int = 25, start_date: str = None, end_date: str = None, terminal_id: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TSWVERS"
    count_str = "SELECT COUNT(*) FROM TSWVERS"
    if start_date is not None:
        data_str += " WHERE EFFECTIVE_DATE >= '{}'".format(start_date)
        count_str += " WHERE EFFECTIVE_DATE >= '{}'".format(start_date)
    if end_date is not None:
        data_str += " AND EFFECTIVE_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE EFFECTIVE_DATE <= '{}'".format(end_date)
        count_str += " AND EFFECTIVE_DATE <= '{}'".format(
            end_date) if start_date is not None else " WHERE EFFECTIVE_DATE <= '{}'".format(end_date)
    if terminal_id is not None:
        data_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
        count_str += " AND TERMINAL_ID LIKE '%{}%'".format(
            terminal_id) if start_date is not None or end_date is not None else " WHERE TERMINAL_ID LIKE '%{}%'".format(terminal_id)
    data_str += " ORDER BY ID DESC LIMIT {} OFFSET {}".format(
        limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }


@router.get("/terminals/types")
async def get_terminal_types(page: int = 1, limit: int = 25, control_rom: str = None):
    db = sqlite3.db_connection(db_location)
    db.connect()
    data_str = "SELECT * FROM TERMTYP"
    count_str = "SELECT COUNT(*) FROM TERMTYP"
    if control_rom is not None:
        data_str += " WHERE CONTROL_ROM_EDITION LIKE '%{}%'".format(
            control_rom)
        count_str += " WHERE CONTROL_ROM_EDITION LIKE '%{}%'".format(
            control_rom)
    data_str += " ORDER BY ID LIMIT {} OFFSET {}".format(limit, (page-1)*limit)
    data = db.execute(data_str)
    count = db.execute(count_str)
    db.close()
    return {
        "data": data or [],
        "count": count[0]['COUNT(*)']
    }
