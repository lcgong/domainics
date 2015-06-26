import unittest

from tornice.util import ContentTree

class TestDAG(unittest.TestCase):

    def test_1(self):

        dag = ContentTree()

        a, b, c, d, e, f, g, h = (100 + i for i in range(0, 8))
        A, B, C, D, E, F, G, H = 'ABCDEFGH'

        """
        a(100) -> b(101) -> d(103) -> e(104)
        |         |         + ------> h(107)
        |         + ------> g(106)
        +  -----> c(102) -> f(105)

        """
        dag.set(a, A)
        dag.set(b, B, a)
        dag.set(c, C, a)
        dag.set(d, D, b)
        dag.set(e, E, d)
        dag.set(h, H, d)
        dag.set(g, G, b)
        dag.set(f, F, c)

        self.assertEqual(list(dag[p] for p in dag.upwards(e)), ['D', 'B', 'A'])
        self.assertEqual(list(dag[p] for p in dag.upwards(f)), ['C', 'A'])        

        with self.assertRaises(ValueError):
            dag.unset(d) # the point d has children.

        self.assertEqual(set(dag.children(d)), set([e, h]))
        dag.unset(e)
        self.assertIsNone(dag[e])
        self.assertEqual(set(dag.children(d)), set([h]))
        dag.unset(h)
        dag.unset(d)
        self.assertEqual(dag.parent(f), c)


if __name__ == '__main__':
    unittest.main()
 
