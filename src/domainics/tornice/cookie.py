# -*- coding: utf-8 -*-


import tempfile
import os
import stat
import uuid
import base64

def generate_cookie_secret(preserve=True):


    if preserve:
        tmpdir = os.path.join(tempfile.gettempdir(), 'tornice')
        os.makedirs(tmpdir, 0o700, exist_ok=True)

        secretfile = os.path.join(tmpdir, 'COOKIE_SECRET')
        if os.path.exists(secretfile):
            with open(secretfile, 'r') as f:
                cookie_secret = f.readline()
            os.chmod(secretfile, stat.S_IWUSR|stat.S_IRUSR)
            if cookie_secret is not None:
                return cookie_secret

    secret = base64.b64encode(uuid.uuid4().bytes + uuid.uuid4().bytes)
    secret = secret.decode('UTF-8')

    if secret:
        with open(secretfile, 'w') as f:
            f.write(secret)

    return secret
