# coding: utf-8
import inspect


class Test:
    name = 'aa'
    data = 'bb'

    @staticmethod
    def get_line_no():
        return inspect.stack()[0][2]

    @classmethod
    def get_class_name(cls):
        return inspect.stack()[1][3]

    def m1(self, name):
        self.name = name
        print (self.data, name)

    @classmethod
    def m2(cls, name):
        cls.name = name
        print(cls.data, name)

    @staticmethod
    def m3(name):
        print(name)

class Test2(Test):
    @classmethod
    def m4(cls):
        print(Test2.get_line_no())

t = Test2()
t.m1('kk')
Test.m2('nn')
Test.m3('ll')
print(t.name)
print(Test.name)
print(Test2.name)
print(Test2.m4())