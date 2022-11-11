"""Convert team-level queries to the equivalent project-level queries."""

import argparse
import hashlib
import logging
import pprint
import sys

from CheckmarxPythonSDK.CxPortalSoapApiSDK import get_query_collection, upload_queries
from CheckmarxPythonSDK.CxRestAPISDK import ProjectsAPI, ScansAPI, TeamAPI

_version = '0.1.0'

# Logging
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
logger = logging.getLogger('QueryConverter')
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
formatter = logging.Formatter(fmt='%(asctime)s: %(levelname)s: %(name)s: %(msg)s')
handler.setFormatter(formatter)

# Constants
CX_DESCRIPTION_ID = 'CxDescriptionID'
DESCRIPTION = 'Description'
ERROR_MESSAGE = 'ErrorMessage'
IMPACTS = 'Impacts'
IS_ENCRYPTED = 'IsEncrypted'
IS_READONLY = 'IsReadOnly'
IS_SUCCESFULL = 'IsSuccesfull'
LANGUAGE = 'Language'
LANGUAGE_NAME = 'LanguageName'
LANGUAGE_STATE_DATE = 'LanguageStateDate'
NAME = 'Name'
OWNING_TEAM = 'OwningTeam'
PACKAGE_FULL_NAME = 'PackageFullName'
PACKAGE_ID = 'PackageId'
PACKAGE_TYPE = 'PackageType'
PACKAGE_TYPE_NAME = 'PackageTypeName'
PROJECT = 'Project'
PROJECT_ID = 'ProjectId'
QUERIES = 'Queries'
QUERY_GROUPS = 'QueryGroups'
QUERY_ID = 'QueryId'
QUERY_VERSION_CODE = 'QueryVersionCode'
SOURCE = 'Source'
STATUS = 'Status'
TEAM = 'Team'

projects_api = ProjectsAPI()
scans_api = ScansAPI()
team_api = TeamAPI()


class QueryCollection:
    '''A collection of CxQL queries augmented with other data from the CxSAST instance.'''

    def __init__(self, options):

        # Configuration options
        self.options = options
        # A mapping from team id to associated projects
        self.team_project_map = self.create_team_project_map()
        # A mapping from team ids to lists of ancestor team ids
        self.team_ancestry_map = self.create_team_ancestry_map()
        # A list of CxQL query groups, each potentially containing CxQL queries
        self.query_groups = self.retrieve_query_groups()
        # A mapping of project ids to scanned languages (built on demand)
        self.project_language_map = {}

    def get_query_groups(self):

        return self.query_groups

    def create_team_project_map(self):
        '''Creates a mapping from team ids to projects.'''
        logger.debug('Creating team->project map')
        team_project_map = {}
        for proj in projects_api.get_all_project_details():
            proj_list = team_project_map.get(proj.team_id, [])
            proj_list.append(proj.project_id)
            team_project_map[proj.team_id] = proj_list

        logger.debug(f'Team->project map: {team_project_map}')
        return team_project_map

    def create_team_ancestry_map(self):

        team_ancestry_map = {}

        all_teams = team_api.get_all_teams()
        for team in all_teams:
            ancestry = []
            parent_id = team.parent_id
            while parent_id > 0:
                ancestry.append(parent_id)
                for team2 in all_teams:
                    if team2.team_id == parent_id:
                        parent_id = team2.parent_id
            team_ancestry_map[team.team_id] = ancestry

        logger.debug(f'team_ancestry_map: {team_ancestry_map}')
        return team_ancestry_map

    def retrieve_query_groups(self):
        '''Retrieves CxQL queries from CxSAST.'''
        logger.debug('Retrieving queries...')
        resp = get_query_collection()
        if not resp[IS_SUCCESFULL]:
            logger.error(f'Error retrieving queries: {resp[ERROR_MESSAGE]}')
            sys.exit(1)

        query_groups = [qg for qg in resp[QUERY_GROUPS]
                        if ((qg[PACKAGE_TYPE] == PROJECT) or
                            (qg[PACKAGE_TYPE] == TEAM and
                             (not self.options.teams or
                              qg[OWNING_TEAM] in self.options.teams)))]

        return query_groups

    def create_new_query_groups(self):
        new_query_groups = []

        for qg in self.query_groups:
            if qg[PACKAGE_TYPE] == TEAM:
                logger.debug(f'Processing query group {qg[NAME]} for team {qg[OWNING_TEAM]}')
                for project_id in self.team_project_map[qg[OWNING_TEAM]]:
                    project_languages = self.get_project_languages(project_id)
                    if qg[LANGUAGE] not in project_languages:
                        logger.debug(f'  {qg[LANGUAGE]} not in project {project_id} languages ({project_languages})')
                        continue
                    add_query_group = False
                    pqg = self.find_project_query_group(new_query_groups, project_id)
                    if pqg:
                        logger.debug(f'  Query group already exists for project {project_id}')
                    else:
                        logger.debug(f'  Creating new query group for project {project_id}')
                        pqg = self.create_project_query_group(qg, project_id)
                        add_query_group = True

                    for q in qg[QUERIES]:
                        logger.debug(f'  Processing query {q[NAME]}')
                        # Has this query already been customized at the project level?
                        oldpg = self.find_project_query_group(query_groups, project_id)
                        if oldpg and self.find_query_by_name(oldpg, q[NAME]):
                            logger.debug(f'    Query {q[NAME]} already customized at project level')
                            continue
                        logger.debug(f'    Adding query {q[NAME]} to query group')

                        if oldpg:
                            q[PACKAGE_ID] = oldpg[PACKAGE_ID]
                        else:
                            q[PACKAGE_ID] = -1
                        q[QUERY_ID] = 0
                        q[QUERY_VERSION_CODE] = 0
                        q[STATUS] = 'New'
                        pqg[QUERIES].append(q)

                    # Only add the query group if it has one or more queries
                    if pqg[QUERIES] and add_query_group:
                        logger.debug(f'Adding query group {pqg[NAME]}')
                        new_query_groups.append(pqg)

        return new_query_groups

    def get_project_languages(self, project_id):

        if project_id not in self.project_language_map:
            scans = scans_api.get_all_scans_for_project(project_id,
                                                        "Finished",
                                                        1)
            if scans:
                languages = []
                for language in scans[0].scan_state.language_state_collection:
                    languages.append(language.language_id)
                self.project_language_map[project_id] = languages
            else:
                logger.warn(f'No scans found for project {project_id}')

        return self.project_language_map[project_id]


def save_query_groups(query_groups):
    '''Saves the specified query groups back to CxSAST.'''
    logger.debug('Saving query groups')
    resp = upload_queries(query_groups)
    if not resp[IS_SUCCESFULL]:
        logger.error(f'Error uploading queries: {resp[ERROR_MESSAGE]}')
        sys.exit(1)


def validate_query_groups(query_groups, new_query_groups):
    """Make sure that all the query groups and queries in
    new_query_groups are in query_groups."""
    qg_total = 0
    qg_failed = 0
    q_total = 0
    q_failed = 0
    for qg in new_query_groups:
        logger.debug(f'Checking query group {qg[PACKAGE_FULL_NAME]}')
        qg_total = qg_total + 1
        qg1 = find_query_group(query_groups, qg)
        if qg1:
            logger.debug(f'Found query group {qg1[PACKAGE_FULL_NAME]}')
            for q in qg[QUERIES]:
                q_total = q_total + 1
                q1 = find_query(qg1, q)
                if q1:
                    logger.debug(f'Found query {q[NAME]}')
                else:
                    logger.error(f'Query {q[NAME]} not found')
                    q_failed = q_failed + 1
        else:
            logger.error(f'Query group {qg[PACKAGE_FULL_NAME]} not found.')
            qg_failed = qg_failed + 1

    logger.info(f'Total query_groups: {qg_total}, total queries: {q_total}')
    if qg_failed or q_failed:
        logger.error(f'Failed query groups: {qg_failed}, failed queries: {q_failed}')


def convert_queries(options):

    query_collection = QueryCollection(options)
    query_groups = query_collection.get_query_groups()

    if options.debug:
        dump_query_groups(query_groups, 'Old query groups')
    new_query_groups = query_collection.create_new_query_groups()
    if options.debug:
        dump_query_groups(new_query_groups, 'New query groups')
    if options.pretty_print:
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(new_query_groups)
    if not options.dry_run:
        save_query_groups(new_query_groups)
        new_query_collection = QueryCollection(options)
        query_groups = new_query_collection.get_query_groups()
        validate_query_groups(query_groups, new_query_groups)


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
    logger.debug(f'Searching query groups for {query_group[PACKAGE_FULL_NAME]}')
    for qg in query_groups:
        logger.debug(f'  qg[PACKAGE_FULL_NAME]: {qg[PACKAGE_FULL_NAME]}')
        if qg[PACKAGE_FULL_NAME] == query_group[PACKAGE_FULL_NAME]:
            if qg[PACKAGE_TYPE] == query_group[PACKAGE_TYPE]:
                return qg
            else:
                logger.debug(f'Package types do not match ({qg[PACKAGE_TYPE]} != {query_group[PACKAGE_TYPE]})')

    return None


def find_query(query_group, query):
    """Find the specified query in the specified query group.

    Both the query name and the query source are compared.
    """
    logger.debug(f'Searching query group {query_group[PACKAGE_FULL_NAME]} for query {query[NAME]}')
    for q in query_group[QUERIES]:
        logger.debug(f'  q[NAME]: {q[NAME]}')
        if q[NAME] == query[NAME]:
            if q[SOURCE] == query[SOURCE]:
                return q
            else:
                logger.debug(f'  Found {q[NAME]} but source code is different')


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
    if tqg[DESCRIPTION]:
        nqg[DESCRIPTION] = tqg[DESCRIPTION]
    else:
        nqg[DESCRIPTION] = ''
    nqg[IMPACTS] = []
    nqg[IS_ENCRYPTED] = tqg[IS_ENCRYPTED]
    nqg[IS_READONLY] = False
    nqg[LANGUAGE] = tqg[LANGUAGE]
    nqg[LANGUAGE_NAME] = tqg[LANGUAGE_NAME]
    nqg[LANGUAGE_STATE_DATE] = tqg[LANGUAGE_STATE_DATE]
    nqg[NAME] = tqg[NAME]
    nqg[OWNING_TEAM] = 0
    nqg[PACKAGE_FULL_NAME] = f'{tqg[LANGUAGE_NAME]}:CxProject_{project_id}:{tqg[NAME]}'
    nqg[PACKAGE_ID] = 0
    nqg[PACKAGE_TYPE] = PROJECT
    nqg[PACKAGE_TYPE_NAME] = f'CxProject_{project_id}'
    nqg[PROJECT_ID] = project_id
    nqg[QUERIES] = []
    nqg[STATUS] = tqg[STATUS]

    return nqg


# Debugging functions
def dump_query_groups(query_groups, message):
    print('------------------------------')
    print(f'{message}')
    print('------------------------------')
    for qg in query_groups:
        print(f'Name: {qg[NAME]}')
        print(f'  Language       : {qg[LANGUAGE_NAME]}')
        print(f'  OwningTeam     : {qg[OWNING_TEAM]}')
        print(f'  PackageFullName: {qg[PACKAGE_FULL_NAME]}')
        print(f'  PackageType    : {qg[PACKAGE_TYPE]}')
        print(f'  ProjectId      : {qg[PROJECT_ID]}')
        print(f'  Status         : {qg[STATUS]}')
        dump_queries(qg[QUERIES])


def dump_queries(queries):
    i = 0
    for q in queries:
        print(f'    [{i}] Name  : {q[NAME]}')
        md5 = hashlib.md5()
        md5.update(q[SOURCE].encode('utf-8'))
        print(f'        MD5   : {md5.hexdigest()}')
        print(f'        Status: {q[STATUS]}')
        i = i + 1


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', dest='debug',
                        default=False)
    parser.add_argument('--dry-run', action='store_true', dest='dry_run',
                        default=False)
    parser.add_argument('--pretty-print', action='store_true',
                        dest='pretty_print', default=False)
    parser.add_argument('teams', type=int, nargs='*')
    args = parser.parse_args()
    convert_queries(args)
