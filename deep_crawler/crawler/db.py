"""
Enhanced database connection handling for 20k+ website parallel processing.
FIXED: Better connection pool sizing, validation, retry logic.
"""
import logging
import time
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2 import OperationalError, InterfaceError
from crawler.settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

log = logging.getLogger("db")

# FIXED: Larger connection pool for 5 parallel browsers + processing
# minconn=10: Always keep 10 connections ready
# maxconn=50: Can handle bursts during parallel processing
POOL = ThreadedConnectionPool(
    minconn=10,
    maxconn=50,
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    # ADDED: Connection validation settings
    connect_timeout=30,
    application_name="web_scraper_crawler"
)

def run(sql: str, params=None, fetch=False, retries=3):
    """
    ENHANCED: Execute SQL with connection validation and retry logic.
    
    Args:
        sql: SQL query string
        params: Query parameters  
        fetch: Whether to return results
        retries: Number of retry attempts for failed connections
    """
    attempt = 0
    last_error = None
    
    for attempt in range(retries):
        conn = None
        try:
            conn = POOL.getconn()
            
            # ADDED: Validate connection is still alive
            if conn.closed != 0:
                log.warning(f"Connection was closed, getting new one (attempt {attempt + 1})")
                POOL.putconn(conn, close=True)  # Force close bad connection
                continue
            
            # Execute query with proper transaction handling
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SET statement_timeout = %s", ("30000",))  # 30s timeout
                    log.debug("SQL %s %s", sql, params)
                    cur.execute(sql, params or ())
                    
                    if fetch:
                        results = cur.fetchall()
                        log.debug(f"Query returned {len(results)} rows")
                        return results
                    else:
                        if cur.rowcount >= 0:
                            log.debug(f"Query affected {cur.rowcount} rows")
                        return None
            
        except (OperationalError, InterfaceError) as e:
            last_error = e
            log.warning(f"Database connection error (attempt {attempt + 1}/{retries}): {e}")
            
            if conn:
                try:
                    POOL.putconn(conn, close=True)  # Force close bad connection
                    conn = None
                except:
                    pass
            
            if attempt < retries - 1:
                # Wait before retry with exponential backoff
                wait_time = (2 ** attempt) * 0.5  # 0.5, 1, 2 seconds
                log.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            
        except Exception as e:
            # Non-connection errors - don't retry
            log.error("SQL error: %s", e, exc_info=True)
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            raise
            
        finally:
            # Return connection to pool (if we got one)
            if conn:
                try:
                    POOL.putconn(conn)
                except Exception as pool_error:
                    log.error(f"Error returning connection to pool: {pool_error}")
    
    # All retries failed
    if last_error:
        log.error(f"All {retries} database connection attempts failed")
        raise last_error
    else:
        raise RuntimeError(f"Database operation failed after {retries} attempts")

def get_pool_status():
    """Get current connection pool status for monitoring"""
    try:
        # Note: These are private attributes, use carefully
        return {
            'minconn': POOL.minconn,
            'maxconn': POOL.maxconn,
            'closed': hasattr(POOL, 'closed') and POOL.closed
        }
    except Exception as e:
        log.error(f"Error getting pool status: {e}")
        return {'error': str(e)}

def test_connection():
    """Test database connection for health checks"""
    try:
        result = run("SELECT 1 as test", fetch=True)
        return result and result[0][0] == 1
    except Exception as e:
        log.error(f"Database connection test failed: {e}")
        return False

def close_pool():
    """Safely close the connection pool (for shutdown)"""
    try:
        POOL.closeall()
        log.info("Database connection pool closed")
    except Exception as e:
        log.error(f"Error closing connection pool: {e}")