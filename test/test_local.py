
import unittest

from tornice.domobj import dobject, dfield, dlist


from tornice import S, local_env

class TestLocalStack(unittest.TestCase):

    def test_1(self):
        def env(f):
            def wrapper(*args, **kwargs):
                with local_env(request=123, db='db'):
                    return f(*args, **kwargs)
            return wrapper

        @env
        def f1(a):
            self.assertEqual(S.request, 123)
            self.assertEqual(S.db, 'db')

        for i in range(100000):
            f1(100)

if __name__ == '__main__':
    unittest.main()
 