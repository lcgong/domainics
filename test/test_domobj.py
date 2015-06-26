
import unittest

from tornice.domobj import dobject, dfield, dlist

class TestDomainObject(unittest.TestCase):

    def test_dobject_eq(self):
        class A(dobject):
            no = dfield(int, expr='1001')

        class B(dobject):
            no = dfield(int, expr='1001')

        a1, a2, b = A(), A(), B()
        self.assertEqual(a1, a2)
        self.assertEqual(a1, b) # ducking equivalent

        class B(dobject):
            no = dfield(int, expr='1001')
            sn = dfield(int, expr='1000')

        a, b = A(), B()
        self.assertNotEqual(a, b)

        b1, b2 = B(), B()
        b1.no = 888
        self.assertEqual(len(b1.__orig__), 1)
        self.assertEqual(b1.__orig__['no'], 1001)

        b1.no = 999 # the original value does not been chaned.
        self.assertEqual(len(b1.__orig__), 1)
        self.assertEqual(b1.__orig__['no'], 1001)

        self.assertNotEqual(b1, b2)     
        b1.no = 1001
        self.assertEqual(len(b1.__orig__), 0)
        self.assertEqual(b1, b2)        

    def test_dfield_is_dobject(self):

        class A(dobject):
            no = dfield(int, expr='1001')

        class B(dobject):
            sn = dfield(int)
            a  = dfield(A, expr="A(no=123)", doc='A domain object')

        b1, b2 = B(sn=1001, a=A(no=2001)), B(sn=1001, a=A(no=2001))
        self.assertEqual(b1, b2)                

        b1, b2 = B(sn=1001, a=A(no=2001)), B(sn=1001, a=A(no=2002))
        self.assertNotEqual(b1, b2)             
        b2.a.no = 2001
        self.assertEqual(b1, b2)


    def test_dlist(self):
        class A(dobject):
            lst = dlist(int,  doc='a string list')

        a = A(no=101)
        print(a)
        self.assertEqual(a.no, 101)
        self.assertEqual(a.lst, [])
        # self.assertEqual(a.lst, [10, 20, 30])


      # self.assertEqual('foo'.upper(), 'FOO')

  # def test_isupper(self):
  #     self.assertTrue('FOO'.isupper())
  #     self.assertFalse('Foo'.isupper())

  # def test_split(self):
  #     s = 'hello world'
  #     self.assertEqual(s.split(), ['hello', 'world'])
  #     # check that s.split fails when the separator is not a string
  #     with self.assertRaises(TypeError):
  #         s.split(2)

if __name__ == '__main__':
    unittest.main()
 