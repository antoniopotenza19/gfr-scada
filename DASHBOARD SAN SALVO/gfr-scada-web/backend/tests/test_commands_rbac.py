def _login(client, username, password):
    r = client.post('/api/auth/login', json={'username': username, 'password': password})
    assert r.status_code == 200
    return r.json()['access_token']


def test_commands_rbac(client_and_db):
    client, _ = client_and_db

    viewer_token = _login(client, 'viewer', 'viewer123!')
    operator_token = _login(client, 'operator', 'operator123!')

    r_viewer = client.post(
        '/api/commands/request',
        json={'command': 'start', 'target': 'COMP1', 'params': ''},
        headers={'Authorization': f'Bearer {viewer_token}'},
    )
    assert r_viewer.status_code == 403

    r_operator = client.post(
        '/api/commands/request',
        json={'command': 'start', 'target': 'COMP1', 'params': ''},
        headers={'Authorization': f'Bearer {operator_token}'},
    )
    assert r_operator.status_code == 200
