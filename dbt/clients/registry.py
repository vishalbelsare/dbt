import requests


def index():
    return requests.get('http://127.0.0.1:4567/api/v1/index.json').json()


def package(name):
    return requests.get(
        'http://127.0.0.1:4567/api/v1/{}.json'
        .format(name)).json()


def package_version(name, version):
    return requests.get(
        'http://127.0.0.1:4567/api/v1/{}/{}.json'
        .format(name, version)).json()


def get_available_versions(name):
    response = package(name)

    return list(response['versions'].keys())
