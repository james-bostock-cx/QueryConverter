"""Convert team-level queries to the equivalent project-level queries."""

import argparse
import datetime
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
        self.create_query_maps()
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
            ancestry = [team.team_id]
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

    def create_query_maps(self):

        logger.debug('Creating query maps')
        # A mapping from project id to a list of custom queries
        self.project_query_map = {}
        # A mapping from query id to owning query group
        self.query_query_group_map = {}
        # A mapping from team id to a list of custom queries
        self.team_query_map = {}

        for qg in self.query_groups:
            logger.debug(f'PackageFullName: {qg[PACKAGE_FULL_NAME]}')
            for q in qg[QUERIES]:
                self.query_query_group_map[q[QUERY_ID]] = qg
                if qg[PACKAGE_TYPE] == PROJECT:
                    project_id = qg[PROJECT_ID]
                    queries = self.project_query_map.get(project_id, [])
                    queries.append(q)
                    self.project_query_map[project_id] = queries
                elif qg[PACKAGE_TYPE] == TEAM:
                    team_id = qg[OWNING_TEAM]
                    queries = self.team_query_map.get(team_id, [])
                    queries.append(q)
                    self.team_query_map[team_id] = queries

    def create_new_query_groups(self):

        logger.debug('Creating new query groups')

        new_query_groups = []

        for project in projects_api.get_all_project_details():

            logger.debug(f'Project {project.project_id} ({project.name})')
            # A mapping from query name to a list of overrides for the query
            query_map = {}
            logger.debug('Project-level queries:')
            for q in self.project_query_map.get(project.project_id, []):
                logger.debug(f'    {q[NAME]}')
                query_map[q[NAME]] = [q]

            for team_id in self.team_ancestry_map[project.team_id]:
                logger.debug(f'Team {team_id} queries:')
                for q in self.team_query_map.get(team_id, []):
                    qg = self.query_query_group_map[q[QUERY_ID]]
                    if qg[LANGUAGE] in self.get_project_languages(project.project_id):
                        logger.debug(f'    {q[NAME]}')
                        entry = query_map.get(q[NAME], [])
                        entry.append(q)
                        query_map[q[NAME]] = entry

            # Now that we have all the overrides for each query, where
            # there are multiple overrides for a query, merge the
            # source code.
            for name in query_map:
                logger.debug(f'Processing query: {name}')
                queries = query_map[name]
                query = queries[0]
                old_qg = self.query_query_group_map[query[QUERY_ID]]
                new_qg_full_name = f'{old_qg[LANGUAGE_NAME]}:CxProject_{project.project_id}:{old_qg[NAME]}'
                new_qg = None
                for qg in new_query_groups:
                    if qg[PACKAGE_FULL_NAME] == new_qg_full_name:
                        new_qg = qg
                        logger.debug(f'{new_qg_full_name} already in new_query_groups')
                        break

                if not new_qg:
                    if old_qg[PACKAGE_TYPE] == PROJECT:
                        logger.debug(f'Reusing existing project query group')
                        new_qg = copy_project_query_group(old_qg)
                    else:
                        logger.debug(f'Creating project query group for {name}')
                        new_qg = create_project_query_group(old_qg, project.project_id)
                        logger.debug(f'new query group name is {new_qg[PACKAGE_FULL_NAME]}.')
                    new_query_groups.append(new_qg)

                logger.debug(f'Query {name} has {len(queries)} overrides')
                if len(queries) > 1:
                    source = self.merge_query_source(name, queries)
                elif old_qg[PACKAGE_TYPE] == TEAM:
                    source = self.create_query_header(old_qg, query) + query[SOURCE]
                else:
                    logger.debug('Skipping project-level query')
                    continue

                if old_qg[PACKAGE_TYPE] == TEAM:
                    # Create a shallow clone of the query to prevent
                    # code above breaking when we set the query ID to
                    # zero.
                    query = dict(query)
                    query[PACKAGE_ID] = -1
                    query[QUERY_ID] = 0
                    query[QUERY_VERSION_CODE] = 0
                    query[STATUS] = 'New'
                query[SOURCE] = source

                in_query_group = False
                for q in qg[QUERIES]:
                    if q[NAME] == name:
                        in_query_group = True
                        break

                if not in_query_group:
                    new_qg[QUERIES].append(query)

        # Only return query groups that have one or more queries
        return [qg for qg in new_query_groups if qg[QUERIES]]

    def merge_query_source(self, name, queries):
        '''Merges the source code of multiple overrides of the same query.

        The name parameter is the name of the query.

        The queries parameter is expected to be a list of queries. If
        there is a project-level override for the query, it is
        expected to be the first entry in the queries list. The
        remaining entries are team-level overrides starting with the
        team lowest in the team hiearchy and ending with the team
        highest in the team hierarchy.

        Each query is converted to a Func delegate.
        '''
        sources = []
        func_name = None
        for q in reversed(queries):
            qg = self.query_query_group_map[q[QUERY_ID]]
            source = q[SOURCE].replace('\n', '\n    ')
            if func_name:
                source = source.replace(f'base.{name}', func_name)
            func_name = qg[PACKAGE_FULL_NAME].replace(':', '_')
            if qg[PACKAGE_TYPE] == TEAM:
                func_name = func_name.replace('_Team_', f'_Team_{qg[OWNING_TEAM]}_')
            header = self.create_query_header(qg, q)
            sources.append(f'{header}\nFunc<CxList> {func_name} = () => {{\n    {source}\n    return result;\n}};\n')

        source = '\n'.join(sources) + f'\nresult = {func_name}();'
        return source

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

    def create_query_header(self, qg, q):

        if qg[PACKAGE_TYPE] == PROJECT:
            owner = f'PROJECT: {qg[PROJECT_ID]} / '
        elif qg[PACKAGE_TYPE] == TEAM:
            owner = f'TEAM: {qg[OWNING_TEAM]} / '
        header = '''// -------------------------------------------------------
// FUSIONED - {package_type} LEVEL
// {owner}
// QUERY: {query_id} / {query_name}
// PACKAGE: {package_id} / {package_name}
// LANGUAGE: {language}
// ON: {date}
// -------------------------------------------------------
'''.format(owner=owner, package_type=qg[PACKAGE_TYPE],
           query_id=q[QUERY_ID], query_name=q[NAME],
           package_id=qg[PACKAGE_ID], package_name=qg[NAME],
           language=qg[LANGUAGE_NAME], date=datetime.datetime.now())

        return header


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


def copy_project_query_group(oqg):
    """Create a project query group for the specified project using
    the specified team query group as a template."""
    nqg = {}
    if oqg[DESCRIPTION]:
        nqg[DESCRIPTION] = oqg[DESCRIPTION]
    else:
        oqg[DESCRIPTION] = ''
    nqg[IMPACTS] = []
    nqg[IS_ENCRYPTED] = oqg[IS_ENCRYPTED]
    nqg[IS_READONLY] = False
    nqg[LANGUAGE] = oqg[LANGUAGE]
    nqg[LANGUAGE_NAME] = oqg[LANGUAGE_NAME]
    nqg[LANGUAGE_STATE_DATE] = oqg[LANGUAGE_STATE_DATE]
    nqg[NAME] = oqg[NAME]
    nqg[OWNING_TEAM] = 0
    nqg[PACKAGE_FULL_NAME] = oqg[PACKAGE_FULL_NAME]
    nqg[PACKAGE_ID] = oqg[PACKAGE_ID]
    nqg[PACKAGE_TYPE] = PROJECT
    nqg[PACKAGE_TYPE_NAME] = oqg[PACKAGE_TYPE_NAME]
    nqg[PROJECT_ID] = oqg[PROJECT_ID]
    nqg[QUERIES] = []
    nqg[STATUS] = oqg[STATUS]

    return nqg


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
        print(f'  PackageId      : {qg[PACKAGE_ID]}')
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
        print(f'        ID                : {q[QUERY_ID]}')
        print(f'        MD5               : {md5.hexdigest()}')
        print(f'        Package ID        : {q[PACKAGE_ID]}')
        print(f'        Query Version Code: {q[QUERY_VERSION_CODE]}')
        print(f'        Status            : {q[STATUS]}')
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
