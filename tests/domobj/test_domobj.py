
import unittest

import decimal
import datetime as dt
from tornice.domobj import dobject, datt, identity, dset



class TestDomainObject(unittest.TestCase):

    @unittest.skip('')
    def test_new(self):
        class A(dobject):
            a = datt(int, expr="100")

        class B(A):
            b = datt(int, expr='201')
            m = datt(int, expr='202')

        class C(B):
            c = datt(int, expr='300')

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
            a_no = datt(str, expr="'100'")
            amt  = datt(float)

            identity(a_no)

        a = A(a_no=200)
        self.assertEqual(a._dobj_id_names, ['a_no'])
        self.assertEqual(a._dobj_id.a_no, 200)
        self.assertEqual(a._dobj_id, (200,))

        class A(dobject):
            a_no = datt(str, expr="'100'")
            b_no = datt(str, expr="'101'")
            amt  = datt(float)

            identity(a_no, b_no)

        a = A(a_no=200, b_no=300)
        self.assertEqual(a._dobj_id_names, ['a_no', 'b_no'])
        self.assertEqual(a._dobj_id.a_no, 200)
        self.assertEqual(a._dobj_id, (200,300))


        class A(dobject):
            a_no = datt(str, expr="'100'")
            b_no = datt(str, expr="'101'")
            amt  = datt(float)

        a = A()
        self.assertEqual(a._dobj_id_names, [])
        self.assertIsNone(a._dobj_id)        


        class A(dobject):
            no1 = datt(int, expr="100")
            no2 = datt(int, expr="101")
            amt = datt(float)
            age = datt(int)

            identity(no1, no2)

        a1 = A(no1=100, no2=200, amt=3333, age=30)
        a2 = A(no1=100, no2=200, amt=2222, age=30)
        a3 = A(no1=100, no2=201, amt=2222, age=30)
        self.assertEqual(a1, a2)
        self.assertNotEqual(a2, a3)

        class A(dobject):
            no1 = datt(int, expr="100")
            no2 = datt(int, expr="101")
            amt = datt(float)
            age = datt(int)

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
            no1 = datt(int, expr="'100'")
            amt = datt(float)

            identity(no1)

        with self.assertRaises(TypeError):
            a = A()
            a.no1 = 123 # the identity attribute is read-only.


        class A(dobject):
            no1 = datt(str, expr="'100'")

        a = A(100) # implict casting
        self.assertEqual(a.no1, '100') 

        a = A(no1=100)
        self.assertEqual(a.no1, '100')

        a = A('100')
        a.no1 = 200 # the identity attr is read-only.
        self.assertEqual(a.no1, '200')


        class A(dobject):
            no1 = datt(float)

        with self.assertRaises(TypeError):
            a = A('dddd')


    @unittest.skip('')
    def test_dset(self):
        class A(dobject):
            n1 = datt(int)
            n2 = datt(int)
            n3 = datt(int)
            
            identity(n1, n2)

        s1 = dset(A)
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
        self.assertEqual(s1[:2], dset([A(100, 100, 901), A(101, 100, 902)]))
        

    @unittest.skip('')
    def test_dset(self):
        class Item(dobject):
            item_no = datt(str)
            amt     = datt(float)
            identity(item_no)

        class Bill(dobject):
            doc_no = datt(str)
            items  = dset(Item)
            identity(doc_no)

        bill = Bill(doc_no='123')
        bill.items.append(Item('no001', 100.0))
        bill.items.append(Item('no002', 110.0))

        items = dset(Item, [Item('no001', 100.0), 
                        Item('no002', 110.0)])

        self.assertEqual(bill.items, items)


        items = dset(Item, [
                        Item('no001', 100.0), 
                        Item('no002', 110.0),
                        Item('no003', 111.0)])

        self.assertNotEqual(bill.items, items)
        bill.items = items
        self.assertEqual(bill.items, items)
        self.assertIsNot(bill.items, items)

        del bill.items[1]
        self.assertEqual(bill.items, [items[0], items[2]])
        bill.items = items
        del bill.items[Item('no001')]
        self.assertEqual(bill.items, [items[1], items[2]])

        bill.items = items
        del bill.items[1:]
        self.assertEqual(bill.items, [items[0]])

        self.assertTrue(bill.items)
        bill.items.clear()
        self.assertFalse(bill.items)

        bill.items += items
        self.assertTrue(bill.items == items)

    @unittest.skip('')
    def test_copy(self):
        class A(dobject):
            a1 = datt(str)
            a2 = datt(int)
            a3 = datt(decimal.Decimal)
            a4 = datt(dt.date)

        a1 = A('abc', 100, '13.4', dt.date(2015,7,1))
        a2 = a1.copy()
        self.assertEqual(a1, a2)
        self.assertIsNot(a1, a2)

        class B(dobject):
            b0 = datt(str)
            b1 = datt(A)

        b1 = B('xyz01')
        b1.b1 = A('abc', 100, '13.4', dt.date(2015,7,1))
        b2 = b1.copy()

        self.assertEqual(b1, b2)
        self.assertIsNot(b1, b2)
        self.assertIsNot(b1.b1, b2.b1)
        self.assertEqual(b1.b1, b2.b1)

        class A(dobject):
            a1 = datt(str)
            a2 = datt(int)

        class B(dobject):
            b1 = datt(dt.date)
            b2 = datt(decimal.Decimal)

            identity(b1)
            
        class C(dobject):
            c1    = datt(int)
            props = datt(A)
            dates = dset(B)

        c1 = C(100)
        c1.props = A('abc', 2100)
        c1.dates.append(B(dt.date(2015,7,1), 120.5))
        c1.dates.append(B(dt.date(2015,7,2), 130.5))    

        c2 = c1.copy()
        self.assertEqual(c1, c2)
        print(c1 , '\n', c2, sep='')

    # @unittest.skip('')
    def test_conform(self):
        class A(dobject):
            a1 = datt(int)
            a3 = datt(int)
            a4 = datt(int)
            identity(a1)

        class B(dobject):
            b1 = datt(int)
            b2 = dset(A)
            b3 = datt(int)
            identity(b1)

        class AA(dobject):
            a1 = datt(int)
            a2 = datt(int)
            a4 = datt(int)
            identity(a1)

        class BB(dobject):
            b1 = datt(int)
            b2 = dset(AA)
            b4 = datt(int)
            identity(b1)

        
        x = B(100)
        x.b2 += [A(211, 213, 1), A(221, 223, 2), A(231, 233, 3)]

        y = BB(500)
        y.b2 += [AA(611, 213, 7), AA(221, 223, 8), AA(631, 233, 9)]
        y.b4 = 501

        print('     x = ', x, sep='') 
        print('     y = ', y, sep='')
        x <<= y # conform x to y
        print('x <<= y: ', x, sep='')
        self.assertEqual(x.b1, 500)
        
        self.assertEqual(x.b2[0].a1, 221)
        self.assertEqual(x.b2[0].a4, 8)

        self.assertEqual(x.b2[2].a1, 611)
        self.assertIsNone(x.b2[2].a3)
        self.assertEqual(x.b2[2].a4, 7)



            

    # def test_dobject_eq(self):
    #     class A(dobject):
    #         no = datt(int, expr='1001')

    #     class B(dobject):
    #         no = datt(int, expr='1001')

    #     a1, a2, b = A(), A(), B()
    #     self.assertEqual(a1, a2)
    #     self.assertEqual(a1, b) # ducking equivalent

    #     class B(dobject):
    #         no = datt(int, expr='1001')
    #         sn = datt(int, expr='1000')

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

    # def test_datt_is_dobject(self):

    #     class A(dobject):
    #         no = datt(int, expr='1001')

    #     class B(dobject):
    #         sn = datt(int)
    #         a  = datt(A, expr="A(no=123)", doc='A domain object')

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
 