import argparse
import json
import sqlite3
import sys
from collections import defaultdict

import mozlog
import requests
import thclient

logger = None

actions = {name:i for i, name in enumerate(["test_result", "log", "crash"])}
actions_by_number = {v:k for k,v in actions.iteritems()}
log_levels = mozlog.structuredlog.log_levels
levels_by_number = {v:k for k,v in log_levels.iteritems()}

class ErrorLine(object):
    def __init__(self, data):
        self.row_id = None
        self.data = data
        self.matches = []
        self.autostar_id = None

    def __str__(self):
        return repr(self.data)

class Job(object):
    def __init__(self, data):
        self.row_id = None
        self.data = data
        self.success = None
        self.error_url = None
        self.error_lines = []

    @property
    def id(self):
        return self.data['id']

def setup_database(conn, repo, revision):
    c = conn.cursor()

    c.execute("""CREATE TABLE results_sets
    (id integer primary key, sha1 text)""")

    c.execute("""CREATE TABLE jobs
    (id integer primary key, errors_processed integer)""")

    c.execute("""CREATE TABLE errors
    (id integer primary key autoincrement, action integer, line integer, test text, subtest text, status text, expected text, message text, stack text, signature text, stackwalk_stdout text, stackwalk_stderr text, level integer, autostar_id integer, job_id integer)""")
    conn.commit()

    c.execute("""CREATE TABLE autostar
    (id integer primary key autoincrement)""")

    c.execute("""CREATE TABLE meta
    (last_revision_id text, repository text)""")

    c.execute("""INSERT INTO meta (repository, last_revision_id)
    VALUES (?,?)""", (repo, revision))
    conn.commit()

def get_meta(conn):
    c = conn.cursor()
    c.execute("""SELECT repository, last_revision_id FROM meta LIMIT 1""")
    row = c.fetchone()

    if not row:
        raise ValueError("Need to set up db with repo and revision")
    else:
        repo, last_rev = row

    return repo, last_rev


def get_result_sets(client, repository, last_rev):
    # Just assume there are < 100 changes for now
    if repository == "try":
        count = 1
        params = {"revision": last_rev}
    else:
        count = 100
        params = {"fromchange": last_rev}
    return client.get_resultsets(repository, count=count, **params)


def get_jobs(client, repository, result_set):
    return client.get_jobs(repository, result_set_id=result_set['id'], count=None)


def insert_jobs(conn, jobs):
    rv= []
    c = conn.cursor()
    for job in jobs:
        c.execute("""INSERT OR IGNORE INTO jobs (id, errors_processed) VALUES (?, ?)""",
                  (job.id, 0))
        job.row_id = c.lastrowid
    conn.commit()
    return rv


def job_has_errors(job):
    return job.data["result"] in ["testfailed", "busted"]


def get_artifacts(client, repository, job):
    return client.get_artifacts(project=repository, job_id=job.id,
                                name="Job Info")


def get_error_url(artifacts):
    for item in artifacts:
        data = item.get("blob", {}).get("job_details", [])
        for item in data:
            logger.debug("Processing artifact %s %s" % (item.get("title", None), item.get("value")))
            if (item.get("content_type") == "link" and
                item.get("title") == "artifact uploaded" and
                item.get("value").endswith("_errorsummary.log")):

                logger.debug("Got error url %s" % item["url"])
                return item["url"]


def get_jobs_by_type(client, repository, result_set):
    """Return a dictionary of {job name: [(job_data, error_url)]} for all jobs in
    a result set"""
    rv = defaultdict(list)
    jobs = get_jobs(client, repository, result_set)
    logger.debug("Found %i jobs" % len(jobs))
    for job_data in jobs:
        job = Job(job_data)
        logger.debug("Loading job %s %s %s" % (job.id, job.data["ref_data_name"], job.data["result"]))
        if job_has_errors(job):
            artifacts = get_artifacts(client, repository, job)
            # if job.data["build_system_type"] == "buildbot":
            #     import pdb
            #     pdb.set_trace()
            job.error_url = get_error_url(artifacts)

        rv[job_data["ref_data_name"]].append(job)

    return rv


def fetch_summary(url):
    resp = requests.get(url)
    resp.raise_for_status()
    for line in resp.iter_lines():
        yield json.loads(line)


def update_meta(conn, last_rev):
    pass


class Matcher(object):
    def __init__(self, conn):
        self.conn = conn

    def __call__(self, error_line):
        pass


class PreciseTestMatcher(Matcher):
    def __call__(self, error_lines):
        c = self.conn.cursor()
        for error_line in error_lines:
            logger.debug("Looking for test match on line %s" % error_line)
            line = error_line.data

            if line["action"] == "test_result":
                query = """SELECT autostar_id FROM errors WHERE
                action = ? AND test = ? AND status = ? AND
                expected = ? AND autostar_id IS NOT NULL"""
                params = [actions[line["action"]], line["test"],
                          line["status"], line["expected"]]

                for maybe_null in ["subtest", "message", "stack"]:
                    value = line.get(maybe_null)
                    if value is None:
                        query += " AND %s IS NULL" % maybe_null
                    else:
                        query += " AND %s = ?" % maybe_null
                        params.append(value)

                query += " GROUP BY autostar_id"
                c.execute(query, tuple(params))

                rows = c.fetchall()

                logger.debug("Found %i matching rows" % len(rows))

                assert len(rows) <= 1, "Need to fix the case where we have more than one exact match for a test failure"
                if rows:
                    logger.info("PreciseTestMatcher matched line %s" % error_line)
                    error_line.matches.append((rows[0][0], 1))


class PreciseLogMatcher(Matcher):
    def __call__(self, error_lines):
        c = self.conn.cursor()
        for error_line in error_lines:
            line = error_line.data
            if line["action"] == "log":
                c.execute("""SELECT autostar_id FROM errors WHERE
                action = ? AND level = ? AND message = ?
                GROUP BY autostar_id
                """,
                          (actions["log"], line["level"], line.get("message")))
                rows = c.fetchall()

                assert len(rows) <= 1, "Need to fix the case where we have more than one exact match for a log message"
                if rows:
                    logger.info("PreciseLogMatcher matched line %s" % error_line)
                    error_line.matches.append((rows[0][0], 1))

def get_creators():
    return [test_failure_creator]

def test_failure_creator(error_lines):
    rv = []
    for i, error_line in enumerate(error_lines):
        if ("test" in error_line.data and "subtest" in error_line.data and
            "status" in error_line.data and "expected" in error_line.data):
            rv.append(i)
    return rv

def get_job_types_with_errors(client, repository, result_set):
    jobs_by_type = get_jobs_by_type(client, repository, result_set)
    rv = {}
    for key, value in jobs_by_type.iteritems():
        if any(job.error_url is not None for job in value) is not None:
            rv[key] = value
    return rv


def get_matchers(conn):
    return [PreciseTestMatcher(conn), PreciseLogMatcher(conn)]


def get_error_lines(summary_url):
    #Return a list of (line, [matches]) where [matches] is always empty
    error_lines = []
    if summary_url:
        for error_line in fetch_summary(summary_url):
            error_lines.append(ErrorLine(error_line))
    return error_lines


def insert_errors(conn, job):
    c = conn.cursor()
    rv = []
    for error_line in job.error_lines:
        line = error_line.data
        c.execute("""INSERT INTO errors (action, line, test, subtest, status, expected, message, stack, signature, stackwalk_stdout, stackwalk_stderr, level, job_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (actions[line["action"]], line["line"], line.get("test"),
                   line.get("subtest"), line.get("status"), line.get("expected"),
                   line.get("message"), line.get("stack"), line.get("signature"),
                   line.get("stackwalk_stdout"), line.get("stackwalk_stderr"),
                   log_levels.get(line.get("level")), job.id))
        error_line.row_id = c.lastrowid
        c.execute("""UPDATE jobs SET errors_processed = 1 WHERE id = ?""", (job.id,))
    conn.commit()


def match_errors(matchers, job):
    error_lines = [line for line in job.error_lines if not line.autostar_id]
    for matcher in matchers:
        matcher(error_lines)

    for error_line in job.error_lines:
        best_match_id = get_best_match(error_line)
        if best_match_id:
            error_line.autostar_id = best_match_id
            logger.info("Got a match for error line %s with autostar id %i" % (error_line, error_line.autostar_id))

def get_best_match(error_line):
    if not error_line.matches:
        return

    ordered_matches = sorted(error_line.matches[:], key=lambda x:x[1])
    return ordered_matches[-1][0]

def insert_matches(conn, job):
    c = conn.cursor()
    for error_line in job.error_lines:
        # Should make this only update if the autostar id was previous None
        if error_line.autostar_id:
            c.execute("""UPDATE errors SET autostar_id = ? WHERE id = ?""",
                      (error_line.autostar_id, error_line.row_id))
    conn.commit()

new_matched_tests = set()

def add_new_intermittents(conn, jobs_by_type):
    all_failed_jobs = [item for item in
                       reduce(lambda x,y:x+y, jobs_by_type.values(), [])
                       if not item.success]
    logger.info("Looking for intermittents in %i jobs" % len(all_failed_jobs))
    for type, jobs in jobs_by_type.iteritems():
        # The approach here is currently to look for new intermittents to add, one at a time
        # and then rerun the matching on other jobs
        # TODO: limit the possible matches to those that have just been added
        logger.debug("%s: %i jobs" % (type, len(jobs)))
        if len(jobs) <= 1:
            logger.debug("Too few jobs in the current set")
            continue

        # For now conservatively assume that we can only mark new intermittents if
        # one run in the current set fully passes
        if not any(job.success for job in jobs):
            logger.debug("No successful jobs to compare against")
            continue

        for i, job in enumerate(jobs):
            logger.debug("Processing job %i" % i)
            if job.success:
                continue

            found_new_matches = False

            for creator in get_creators():
                unmached_lines = [item for item in job.error_lines if item.autostar_id is None]
                if unmached_lines:
                    logger.debug("Found %i unmached lines" % len(unmached_lines))
                line_indicies = creator(unmached_lines)
                for index in line_indicies:
                    error_line = job.error_lines[index]
                    found_new_matches = True
                    new_matched_tests.add(error_line.data["test"])
                    logger.info("Found new intermittent %s" % error_line)
                    c = conn.cursor()
                    c.execute("""INSERT INTO autostar DEFAULT VALUES""")
                    error_line.autostar_id = c.lastrowid
                    assert error_line.autostar_id is not None
                    assert error_line.row_id is not None
                    c.execute("""UPDATE errors SET autostar_id = ? WHERE id = ?""",
                              (error_line.autostar_id, error_line.row_id))
                    conn.commit()
            if found_new_matches:
                for rematch_job in all_failed_jobs:
                    logger.debug(rematch_job)
                    if rematch_job == job:
                        continue
                    logger.debug("Trying rematch on job %i (%s)" % (rematch_job.id, rematch_job.data['ref_data_name']))
                    match_errors(get_matchers(conn), rematch_job)
                    insert_matches(conn, rematch_job)

def job_errors_processed(conn, job):
    c = conn.cursor()
    c.execute("""SELECT errors_processed FROM jobs WHERE id = ?""",
              (job.id,))
    return bool(c.fetchone()[0])

def load_error_lines(conn, job):
    c = conn.cursor()
    c.execute("""SELECT id, action, line, test, subtest, status, expected, message, stack, signature, stackwalk_stdout, stackwalk_stderr, level, autostar_id FROM errors WHERE job_id = ?""",
              (job.id,))
    rows = c.fetchall()
    cols = [item[0] for item in c.description]

    rv = []
    for row in rows:
        row_data = {cols[i]:row[i] for i in xrange(len(cols))}

        row_id = row_data.pop("id")
        autostar_id = row_data.pop("autostar_id")

        action = actions_by_number[row_data["action"]]

        if action == "test_result":
            data = {"test": row_data.get("test"),
                    "subtest": row_data.get("subtest"),
                    "status": row_data.get("status"),
                    "expected": row_data.get("expected"),
                    "message": row_data.get("message"),
                    "stack": row_data.get("stack")}
        elif action == "crash":
            data = {"test": row_data.get("test"),
                    "signature": row_data.get("signature"),
                    "stackwalk_stdout": row_data.get("stackwalk_stdout"),
                    "stackwalk_stderr": row_data.get("stackwalk_stderr")}
        elif action == "log":
            data = {"level": levels_by_number[row_data.get("level")],
                    "message": row_data.get("message")}
        else:
            logger.critical("Unrecognised action %s" % action)
            sys.exit(1)

        data["action"] = action
        data["line"] = row_data["line"]

        error_line = ErrorLine(data)
        error_line.row_id = row_id
        error_line.autostar_id = autostar_id
        rv.append(error_line)
    return rv

def process_errors(conn, job):
    if not job_errors_processed(conn, job):
        job.error_lines = get_error_lines(job.error_url)
        insert_errors(conn, job)
        job.success = False
    else:
        job.success = False
        job.error_lines = load_error_lines(conn, job)
    match_errors(get_matchers(conn), job)
    insert_matches(conn, job)


def update_meta(conn, new_last_rev):
    c = conn.cursor()
    c.execute("""UPDATE meta SET last_revision_id = ?""", (new_last_rev,))
    conn.commit()

def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--setup", nargs=2, help="Recreate the database from scratch. Takes two arguments, a")
    mozlog.commandline.add_logging_group(parser)
    return parser

def main():
    global logger
    args = parser().parse_args()
    logger = mozlog.commandline.setup_logging("autostar", args, {"mach": sys.stdout})
    client = thclient.TreeherderClient()
    conn = sqlite3.connect("star.db")
    try:
        if args.setup:
            setup_database(conn, *args.setup)
            return

        repository, last_rev = get_meta(conn)
        result_sets = get_result_sets(client, repository, last_rev)
        #Process from oldest to newest
        for result_set in reversed(result_sets):
            logger.info("processing result set for revision %s" % result_set['revision'])
            jobs_by_type = get_job_types_with_errors(client, repository, result_set)
            for job_type, jobs in jobs_by_type.iteritems():
                insert_jobs(conn, jobs)
                for job in jobs:
                    if job.error_url:
                        logger.info("processing job %s (%s) errors" % (job.id, job_type))
                        process_errors(conn, job)
                    else:
                        job.success = True

        some_failed = {key:value for key, value in jobs_by_type.iteritems()
                       if not all(item.success for item in value)}
        add_new_intermittents(conn, some_failed)
        if repository != "try":
            update_meta(conn, result_sets[0]["revision"])
    finally:
        conn.close()

if __name__ == "__main__":
    main()
