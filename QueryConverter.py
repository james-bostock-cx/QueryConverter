"""Convert team-level queries to the equivalent project-level queries."""

import argparse
import hashlib
import logging
import sys

from CheckmarxPythonSDK.CxPortalSoapApiSDK import get_query_collection, upload_queries
from CheckmarxPythonSDK.CxRestAPISDK import ProjectsAPI

# Logging
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger = logging.getLogger('QueryConverter')
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
formatter = logging.Formatter(fmt='%(asctime)s: %(levelname)s: %(name)s: %(msg)s')
handler.setFormatter(formatter)

# Constants
DESCRIPTION = 'Description'
ERROR_MESSAGE = 'ErrorMessage'
IS_ENCRYPTED = 'IsEncrypted'
IS_READONLY = 'IsReadOnly'
IS_SUCCESFULL = 'IsSuccesfull'
LANGUAGE = 'Language'
LANGUAGE_NAME = 'LanguageName'
LANGUAGE_STATE_DATE = 'LanguageStateDate'
NAME = 'Name'
OWNING_TEAM = 'OwningTeam'
PACKAGE_FULL_NAME = 'PackageFullName'
PACKAGE_TYPE = 'PackageType'
PACKAGE_TYPE_NAME = 'PAckageTypeName'
PROJECT = 'Project'
PROJECT_ID = 'ProjectId'
QUERIES = 'Queries'
QUERY_GROUPS = 'QueryGroups'
QUERY_ID = 'QueryId'
SOURCE = 'Source'
STATUS = 'Status'
TEAM = 'Team'


def create_team_project_map():
    logger.debug('Creating team->project map')
    projects_api = ProjectsAPI()
    team_project_map = {}
    for proj in projects_api.get_all_project_details():
        proj_list = team_project_map.get(proj.team_id, [])
        proj_list.append(proj.project_id)
        team_project_map[proj.team_id] = proj_list

    return team_project_map


def get_query_groups():
    # Retrieve queries from CxSAST
    logger.debug('Retrieving queries...')
    resp = get_query_collection()
    if not resp[IS_SUCCESFULL]:
        logger.error(f'Error retrieving queries: {resp[ERROR_MESSAGE]}')
        sys.exit(1)

    query_groups = [qg for qg in resp[QUERY_GROUPS]
                    if qg[PACKAGE_TYPE] in [TEAM, PROJECT]]

    return query_groups


def create_new_query_groups(query_groups, team_project_map):
    new_query_groups = []

    for qg in query_groups:
        if qg[PACKAGE_TYPE] == TEAM:
            logger.debug(f'Processing query group {qg[NAME]} for team {qg[OWNING_TEAM]}')
            for project_id in team_project_map[qg[OWNING_TEAM]]:
                add_query_group = False
                pqg = find_project_query_group(new_query_groups, project_id)
                if pqg:
                    logger.debug(f'  Query group already exists for project {project_id}')
                else:
                    logger.debug(f'  Creating new query group for project {project_id}')
                    pqg = create_project_query_group(qg, project_id)
                    add_query_group = True

                for q in qg[QUERIES]:
                    logger.debug(f'  Processing query {q[NAME]}')
                    # Has this query already been customized at the project level?
                    oldpg = find_project_query_group(query_groups, project_id)
                    if oldpg and find_query_by_name(oldpg, q[NAME]):
                        logger.debug(f'    Query {q[NAME]} already customized at project level')
                        continue
                    logger.debug(f'    Adding query {q[NAME]} to query group')
                    pqg[QUERIES].append(q)

                # Only add the query group if it has one or more queries
                if pqg[QUERIES] and add_query_group:
                    logger.debug(f'Adding query group {pqg[NAME]}')
                    new_query_groups.append(pqg)

    return new_query_groups


# Utility functions
def find_project_query_group(query_groups, project_id):
    """Find the query group associated with the specified project_id,
    if any, in the list of query groups."""
    for qg in query_groups:
        if qg[PACKAGE_TYPE] == PROJECT and qg[PROJECT_ID] == project_id:
            return qg

    return None


def find_query_group(query_groups, query_group):
    """Find the specified query_group in the specified list of query
    groups."""
    for qg in query_groups:
        if (qg[NAME] == query_group[NAME] and qg[PACKAGE_TYPE] == query_group[PACKAGE_TYPE]):
            return qg

    return None


def find_query(query_group, query):
    """Find the specified query in the specified query group.

    Both the query name and the query source are compared.
    """
    for q in query_group[QUERIES]:
        if q[NAME] == query[NAME] and q[SOURCE] == query[SOURCE]:
            return q

    return None


def find_query_by_name(query_group, query_name):
    """Find the specified query in the specified query group."""
    for q in query_group[QUERIES]:
        if q[NAME] == query_name:
            return q

    return None


def create_project_query_group(tqg, project_id):
    """Create a project query group for the specified project using
    the specified team query group as a template."""
    nqg = {}
    nqg[DESCRIPTION] = tqg[DESCRIPTION]
    nqg[IS_ENCRYPTED] = tqg[IS_ENCRYPTED]
    nqg[LANGUAGE] = tqg[LANGUAGE]
    nqg[LANGUAGE_NAME] = tqg[LANGUAGE_NAME]
    nqg[LANGUAGE_STATE_DATE] = tqg[LANGUAGE_STATE_DATE]
    nqg[NAME] = tqg[NAME]
    nqg[OWNING_TEAM] = 0
    nqg[PACKAGE_FULL_NAME] = f'{tqg[LANGUAGE_NAME]}:Project_{project_id}:{tqg[NAME]}'
    nqg[PACKAGE_TYPE] = PROJECT
    nqg[PROJECT_ID] = project_id
    nqg[QUERIES] = []

    return nqg


# Debugging functions
def dump_query_groups(query_groups, message):
    print('------------------------------')
    print(f'{message}')
    print('------------------------------')
    for qg in query_groups:
        print(f'Name: {qg[NAME]}')
        print(f'  NewQueryGroup: {qg.get("NewQueryGroup", False)}')
        print(f'  OwningTeam: {qg[OWNING_TEAM]}')
        print(f'  PackageFullName: {qg[PACKAGE_FULL_NAME]}')
        print(f'  PackageType: {qg[PACKAGE_TYPE]}')
        print(f'  ProjectId: {qg[PROJECT_ID]}')
        dump_queries(qg[QUERIES])


def dump_queries(queries):
    i = 0
    for q in queries:
        print(f'    [{i}] Name: {q[NAME]}')
        md5 = hashlib.md5()
        md5.update(q[SOURCE].encode('utf-8'))
        print(f'         MD5: {md5.hexdigest()}')
        i = i + 1


class Options:

    def __init__(self):

        self.debug = False
        self.dry_run = False

    def __str__(self):

        return f'Options[debug={self.debug},dry_run={self.dry_run}]'


def save_query_groups(query_groups):
    logger.debug('Saving query groups')
    upload_queries(query_groups)


def validate_query_groups(query_groups, new_query_groups):
    """Make sure that all the query groups and queries in
    new_query_groups are in query_groups."""
    for qg in new_query_groups:
        logging.debug(f'Checking query group {qg[NAME]}')
        qg1 = find_query_group(query_groups, qg)
        if qg1:
            logging.debug(f'Found query group {qg[NAME]}')
            for q in qg[QUERIES]:
                q1 = find_query(qg1, q)
                if q1:
                    logging.debug(f'Found query {q[NAME]}')
                else:
                    logging.error(f'Query {q[NAME]} not found')
        else:
            logger.error(f'Query group {qg[NAME]} not found.')


def convert_queries(options):

    team_project_map = create_team_project_map()
    query_groups = get_query_groups()
    if options.debug:
        dump_query_groups(query_groups, 'Old query groups')
    new_query_groups = create_new_query_groups(query_groups, team_project_map)
    if options.debug:
        dump_query_groups(new_query_groups, 'New query groups')
    if not options.dry_run:
        save_query_groups(new_query_groups)
        query_groups = get_query_groups()
        validate_query_groups(query_groups, new_query_groups)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', dest='debug',
                        default=False)
    parser.add_argument('--dry-run', action='store_true', dest='dry_run',
                        default=False)
    args = parser.parse_args()

    options = Options()
    options.debug = args.debug
    options.dry_run = args.dry_run

    convert_queries(options)
