def test_login_ok(client_and_db):
    client, _ = client_and_db
    r = client.post('/api/auth/login', json={'username': 'admin', 'password': 'admin123!'})
    assert r.status_code == 200
    body = r.json()
    assert body.get('access_token')
    assert body.get('token_type') == 'bearer'
