
import unittest




from tornice.lifeline import Lifeline, History, LifelineError




class TestLifeline(unittest.TestCase):
    def test_a(self):
        def f1():
            def f2():
                with hist.confine((a, [1,2,3])):
                    print(301, repr(a), repr(b))
                    self.assertEqual(a._this_object, [1,2,3])
                    self.assertEqual(b._this_object, 'B')
                    a.append(4)
                    self.assertEqual(a._this_object, [1,2,3,4])
                    self.assertTrue(bool(a))
                    self.assertEqual(len(a), 4)
                    # nonlocal a
                    # a += ['a']
                    # self.assertEqual(a[1:], [2,3,4, 'a'])
                    print(303, a)

            hist = History()
            a = Lifeline(hist)
            b = Lifeline(hist)
            print(100, repr(a), repr(b))
            with hist.confine((a, 'A'), (b, 'B')):
                print(201, str(a), str(b))
                self.assertEqual(a._this_object, 'A')
                f2()
                print(202, str(a), str(b))
                print(203, a)
                self.assertNotEqual(a._this_object, [1, 2, 3, 4, 'a'])
                self.assertEqual(a._this_object, 'A')
                
            print(100, str(a), str(b))

        f1()        

    def test_reenter(self):
        hist = History()
        a = Lifeline(hist)
        with hist.confine((a, 'A')):
            with self.assertRaises(LifelineError):
                with hist.confine((a, 'A1')):
                    pass

if __name__ == '__main__':
    unittest.main()
 