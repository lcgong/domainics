
import unittest

import decimal
import datetime as dt
from tornice.domobj import dobject, dattr, identity, doset



class TestDomainObject(unittest.TestCase):

    @unittest.skip('')
    def test_new(self):
        class A(dobject):
            a = dattr(int, expr="100")

        class B(A):
            b = dattr(int, expr='201')
            m = dattr(int, expr='202')

        class C(B):
            c = dattr(int, expr='300')

        c = C(310, 210, a=150)
        self.assertEqual(c.c, 310)
        self.assertEqual(c.b, 210)
        self.assertEqual(c.m, 202)
        self.assertEqual(c.a, 150)

        with self.assertRaises(ValueError) as ex:
            c = C(310, x=150) # the attribute x is not defined


    @unittest.skip('')
    def test_identity(self):

        class A(dobject):
            a_no = dattr(str, expr="'100'")
            amt  = dattr(float)

            identity(a_no)

        a = A(a_no=200)
        self.assertEqual(a._dobj_id_names, ['a_no'])
        self.assertEqual(a._dobj_id.a_no, 200)
        self.assertEqual(a._dobj_id, (200,))

        class A(dobject):
            a_no = dattr(str, expr="'100'")
            b_no = dattr(str, expr="'101'")
            amt  = dattr(float)

            identity(a_no, b_no)

        a = A(a_no=200, b_no=300)
        self.assertEqual(a._dobj_id_names, ['a_no', 'b_no'])
        self.assertEqual(a._dobj_id.a_no, 200)
        self.assertEqual(a._dobj_id, (200,300))


        class A(dobject):
            a_no = dattr(str, expr="'100'")
            b_no = dattr(str, expr="'101'")
            amt  = dattr(float)

        a = A()
        self.assertEqual(a._dobj_id_names, [])
        self.assertIsNone(a._dobj_id)        


        class A(dobject):
            no1 = dattr(int, expr="100")
            no2 = dattr(int, expr="101")
            amt = dattr(float)
            age = dattr(int)

            identity(no1, no2)

        a1 = A(no1=100, no2=200, amt=3333, age=30)
        a2 = A(no1=100, no2=200, amt=2222, age=30)
        a3 = A(no1=100, no2=201, amt=2222, age=30)
        self.assertEqual(a1, a2)
        self.assertNotEqual(a2, a3)

        class A(dobject):
            no1 = dattr(int, expr="100")
            no2 = dattr(int, expr="101")
            amt = dattr(float)
            age = dattr(int)

        a1 = A(no1=100, no2=200, amt=3333, age=30)
        a2 = A(no1=100, no2=200, amt=2222, age=30)
        a3 = A(no1=100, no2=201, amt=2222, age=30)
        a4 = A(no1=100, no2=201, amt=2222, age=30)

        self.assertNotEqual(a1, a2)
        self.assertNotEqual(a2, a3)
        self.assertEqual(a3, a4)

    @unittest.skip('')
    def test_identity_setattr(self):

        class A(dobject):
            no1 = dattr(int, expr="'100'")
            amt = dattr(float)

            identity(no1)

        with self.assertRaises(TypeError):
            a = A()
            a.no1 = 123 # the identity attribute is read-only.


        class A(dobject):
            no1 = dattr(str, expr="'100'")

        a = A(100) # implict casting
        self.assertEqual(a.no1, '100') 

        a = A(no1=100)
        self.assertEqual(a.no1, '100')

        a = A('100')
        a.no1 = 200 # the identity attr is read-only.
        self.assertEqual(a.no1, '200')


        class A(dobject):
            no1 = dattr(float)

        with self.assertRaises(TypeError):
            a = A('dddd')


    @unittest.skip('')
    def test_doset(self):
        class A(dobject):
            n1 = dattr(int)
            n2 = dattr(int)
            n3 = dattr(int)
            
            identity(n1, n2)

        s1 = doset(A)
        s1.append(A(100, 100, 901))
        s1.append(A(101, 100, 902))
        s1.append(A(102, 100, 903))

        # len
        self.assertEqual(len([obj for obj in s1]), 3)
        
        # int index
        self.assertEqual(s1[1], A(101, 100, 902))
        
        # dobject index
        self.assertEqual(s1[A(101, 100)], A(101, 100, 902))
        self.assertEqual(s1.index(A(101, 100)), 1)
        
        # slice
        self.assertEqual(s1[:2], doset([A(100, 100, 901), A(101, 100, 902)]))
        

    @unittest.skip('')
    def test_doset(self):
        class Item(dobject):
            item_no = dattr(str)
            amt     = dattr(float)
            identity(item_no)

        class Bill(dobject):
            doc_no = dattr(str)
            items  = doset(Item)
            identity(doc_no)

        bill = Bill(doc_no='123')
        bill.items.append(Item('no001', 100.0))
        bill.items.append(Item('no002', 110.0))

        items = doset(Item, [Item('no001', 100.0), 
                        Item('no002', 110.0)])

        self.assertEqual(bill.items, items)


        items = doset(Item, [
                        Item('no001', 100.0), 
                        Item('no002', 110.0),
                        Item('no003', 111.0)])

        self.assertNotEqual(bill.items, items)
        bill.items = items
        self.assertEqual(bill.items, items)
        self.assertIsNot(bill.items, items)

        self.assertTrue(bill.items)
        bill.items.clear()
        self.assertFalse(bill.items)

        bill.items += items
        self.assertTrue(bill.items == items)

    # @unittest.skip('')
    def test_copy(self):
        class A(dobject):
            a1 = dattr(str)
            a2 = dattr(int)
            a3 = dattr(decimal.Decimal)
            a4 = dattr(dt.date)

        a1 = A('abc', 100, '13.4', dt.date(2015,7,1))
        a2 = a1.copy()
        self.assertEqual(a1, a2)
        self.assertIsNot(a1, a2)

        class B(dobject):
            b0 = dattr(str)
            b1 = dattr(A)

        b1 = B('xyz01')
        b1.b1 = A('abc', 100, '13.4', dt.date(2015,7,1))
        b2 = b1.copy()

        self.assertEqual(b1, b2)
        self.assertIsNot(b1, b2)
        self.assertIsNot(b1.b1, b2.b1)
        self.assertEqual(b1.b1, b2.b1)

        class A(dobject):
            a1 = dattr(str)
            a2 = dattr(int)

        class B(dobject):
            b1 = dattr(dt.date)
            b2 = dattr(decimal.Decimal)

            identity(b1)
            
        class C(dobject):
            c1    = dattr(int)
            props = dattr(A)
            dates = doset(B)

        c1 = C(100)
        c1.props = A('abc', 2100)
        c1.dates.append(B(dt.date(2015,7,1), 120.5))
        c1.dates.append(B(dt.date(2015,7,2), 130.5))    

        c2 = c1.copy()
        self.assertEqual(c1, c2)
        print(c1 , '\n', c2, sep='')


    # def test_dobject_eq(self):
    #     class A(dobject):
    #         no = dattr(int, expr='1001')

    #     class B(dobject):
    #         no = dattr(int, expr='1001')

    #     a1, a2, b = A(), A(), B()
    #     self.assertEqual(a1, a2)
    #     self.assertEqual(a1, b) # ducking equivalent

    #     class B(dobject):
    #         no = dattr(int, expr='1001')
    #         sn = dattr(int, expr='1000')

    #     a, b = A(), B()
    #     self.assertNotEqual(a, b)

    #     b1, b2 = B(), B()
    #     b1.no = 888
    #     self.assertEqual(len(b1.__orig__), 1)
    #     self.assertEqual(b1.__orig__['no'], 1001)

    #     b1.no = 999 # the original value does not been chaned.
    #     self.assertEqual(len(b1.__orig__), 1)
    #     self.assertEqual(b1.__orig__['no'], 1001)

    #     self.assertNotEqual(b1, b2)     
    #     b1.no = 1001
    #     self.assertEqual(len(b1.__orig__), 0)
    #     self.assertEqual(b1, b2)        

    # def test_dattr_is_dobject(self):

    #     class A(dobject):
    #         no = dattr(int, expr='1001')

    #     class B(dobject):
    #         sn = dattr(int)
    #         a  = dattr(A, expr="A(no=123)", doc='A domain object')

    #     b1, b2 = B(sn=1001, a=A(no=2001)), B(sn=1001, a=A(no=2001))
    #     self.assertEqual(b1, b2)                

    #     b1, b2 = B(sn=1001, a=A(no=2001)), B(sn=1001, a=A(no=2002))
    #     self.assertNotEqual(b1, b2)             
    #     b2.a.no = 2001
    #     self.assertEqual(b1, b2)


    # def test_dlist(self):
    #     class A(dobject):
    #         lst = dlist(int,  doc='a string list')

    #     a = A(no=101)
    #     print(a)
    #     self.assertEqual(a.no, 101)
    #     self.assertEqual(a.lst, [])
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
 