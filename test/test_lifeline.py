
import unittest




from tornice.lifeline import _make_lifeline_class, History, LifelineError, NoBoundObjectError




class TestLifeline(unittest.TestCase):

    def test_1(self):
        hist = History()
        Lifeline = _make_lifeline_class(str)
        a = Lifeline(hist)

        def f1():
            nonlocal a
            print(201, a)
            hist.confine([(a, 'B')])(f2)()
            print(202, a)
        
        def f2():
            nonlocal a
            print(300, a)

        print(101, a)
        hist.confine([(a, 'A')])(f1)()
        print(102, a)

    def test_2(self):
        hist = History()
        Lifeline = _make_lifeline_class(str)
        a = Lifeline(hist)

        def f1():
            nonlocal a
            print(201, a)
            g = hist.confine([(a, 'B')])(f2)()


            
            for r0, r1 in g:
                print('%s $ %d-%s' % (a._this_object, r0, r1))
                # print(g.close)
                # g.close()
                if r0 == 103:
                    break

            print(202, a)
        
        def f2():
            nonlocal a
            for i in range(5):
                yield i + 100, str(a._this_object)
                # raise ValueError()

        print(101, a)
        hist.confine([(a, 'A')])(f1)()
        print(102, a)

    # def test_a(self):
    #     def f1():
    #         hist = History()
            
    #         LifelineA = _make_lifeline_class(list)
    #         LifelineB = _make_lifeline_class(str)
    #         a = LifelineA(hist)
    #         b = LifelineB(hist)

    #         def f2():
    #             nonlocal a
    #             with hist.confine((a, [1,2,3])):
    #                 print(301, repr(a), repr(b))
    #                 self.assertEqual(a._this_object, [1,2,3])
    #                 self.assertEqual(b._this_object, 'B')
    #                 a.append(4)
    #                 self.assertEqual(a._this_object, [1,2,3,4])
    #                 self.assertTrue(bool(a))
    #                 self.assertEqual(len(a), 4)
    #                 print(type(a))
    #                 a += ['a']
    #                 print(type(a))
    #                 self.assertIsInstance(a, LifelineA)
    #                 self.assertEqual(a[1:], [2,3,4, 'a'])
    #                 print(303, a)


    #         print(100, repr(a), repr(b))
    #         with hist.confine((a, ['A']), (b, 'B')):
    #             print(201, str(a), str(b))
    #             self.assertEqual(a._this_object, ['A'])
    #             f2()
    #             print(202, str(a), str(b))
    #             print(203, a)
    #             self.assertNotEqual(a._this_object, [1, 2, 3, 4, 'a'])
    #             self.assertEqual(a._this_object, ['A'])
                
    #         print(100, str(a), str(b))
    #         self.assertIsNone(a._this_object)
    #         with self.assertRaises(NoBoundObjectError):
    #             len(a)

    #     f1()        

    # def test_reenter(self):
    #     hist = History()
    #     a = Lifeline(hist)
    #     with hist.confine((a, 'A')):
    #         with self.assertRaises(LifelineError):
    #             with hist.confine((a, 'A1')):
    #                 pass
    #         with self.assertRaises(TypeError):
    #             hash(a) # unhashable


if __name__ == '__main__':
    unittest.main()
 