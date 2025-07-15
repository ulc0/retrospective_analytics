import os
import ssl
import json
import base64
import urllib2
import collections


### DO NOT MODIFY ###


def valid_file(f):
    supportedFileExts = (".md", ".png", ".jpg", ".jpeg", ".mp4", ".gif", ".rst", ".wav")
    root_dir = 'docs'

    return f['type'] == 'FILE' \
        and f['name'].endswith(supportedFileExts)\
        and f['path'].startswith(root_dir)


def post_request(url, data, token):
    headers = {
        'Authorization': 'Bearer {0}'.format(token),
        'Content-type': 'application/json'
    }
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    if data is not None:
        data = json.dumps(data)
    req = urllib2.Request(url, data, headers=headers)
    req.get_method = lambda: 'POST'

    try:
      response = urllib2.urlopen(req, context=ssl_ctx)
      return True
    except urllib2.HTTPError as e:
      print "Failed to complete post request to publish documentation"
      print "HTTP Status code: " + e.getcode()
      print "Reason: " + e.reason

      return False


def get_request(url, token):
    headers = {
        'Authorization': 'Bearer {0}'.format(token)
    }
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    req = urllib2.Request(url, headers=headers)
    req.get_method = lambda: 'GET'
    response = urllib2.urlopen(req, context=ssl_ctx)
    return response


def decode(s):
    return base64.b64decode(s)


def get_environment_variable(variable):
    if variable not in os.environ:
        raise RuntimeError('"%s" environment variable was not found.' % variable)
    if os.environ.get(variable) == 'https://ci-role-not-found':
        raise RuntimeError('"%s" role was not found.' % variable)
    return os.environ.get(variable)
    

def get_branch():
    return get_environment_variable('JEMMA_BRANCH')


def get_stemma_url():
    return get_environment_variable('STEMMA_API')


def get_rosetta_url():
    return get_environment_variable('ROSETTA_API')


def get_jemma_url():
    return get_environment_variable('JEMMA_API')


def get_publish_url():
    base_url = '{0}/publish'.format(get_rosetta_url())
    if get_security_mode() != 'INSECURE_MODE':
        base_url = '{0}?repositoryRid={1}'.format(base_url, get_repo_rid())
    return base_url


def get_repo_rid():
    return get_environment_variable('REPOSITORY_RID')


def get_token():
    return get_environment_variable('JOB_TOKEN')


def get_commitish():
    return get_environment_variable('STEMMA_REF')


def get_file_metadata():
    repo_rid = get_repo_rid()
    token = get_token()
    file_paths = '{0}/repos/{1}/paths/contents/?recursive=true'.format(get_stemma_url(), repo_rid)
    file_contents = json.load(get_request(file_paths, token))
    return [f for f in file_contents['directoryContents'] if valid_file(f)]


def get_security_mode():
    repo_rid = get_repo_rid()
    token = get_token()
    jemma_call = '{0}/security/{1}/mode'.format(get_jemma_url(), repo_rid)
    return json.load(get_request(jemma_call, token))


def split_projects(files_metadata):
    projects = collections.defaultdict(list)
    for f in files_metadata:
        path = os.path.normpath(f['path'])
        split_path = path.split(os.sep)
        if len(split_path) >= 3:
            project = split_path[1]
            projects[project] += [f]
    return projects


def get_project_file_payload(files_metadata):
    payload_files = {}
    commitish = get_commitish()
    repo_rid = get_repo_rid()
    token = get_token()
    for f in files_metadata:
        request_url = '{0}/repos/{1}/paths/contents/{2}?recursive=false&commitish={3}'.format(get_stemma_url(), repo_rid, f['path'], commitish)
        file_content_response = json.load(get_request(request_url, token))

        is_binary = file_content_response['metadata']['binary']

        content = decode(file_content_response['fileContents']) if not is_binary else file_content_response['fileContents']

        if not is_binary:
            payload_file = {'markdown': content, 'type': 'markdown'}
        else:
            payload_file = {'media': content, 'type': 'media'}

        payload_files[f['path']] = payload_file

    return payload_files


def get_file_payload():
    files_metadata = get_file_metadata()
    project_file_metadata = split_projects(files_metadata)
    project_payloads = {proj: get_project_file_payload(files_metadata) for proj, files_metadata in project_file_metadata.iteritems()}
    return project_payloads


def publish(payload):
    return post_request(get_publish_url(), payload, get_token())


### RUNNING SCRIPT ###

if get_branch() != 'master':
    raise RuntimeError('Not on "master" branch. Cannot publish.')

print 'Compiling repository files.'
print
project_files = get_file_payload()
for proj, proj_files in project_files.iteritems():
    print 'Starting process for "{0}".'.format(proj)
    print 'Creating bundle for "{0}".'.format(proj)
    pl = {'productId': proj, 'files': proj_files}
    print 'Publishing "{0}".'.format(proj)
    publish_success = publish(pl)
    if not publish_success:
      print 'Publishing "{0}" failed.'.format(proj)
      exit()

    print 'Finished process for "{0}".'.format(proj)

print 'Completed all publishing.'
