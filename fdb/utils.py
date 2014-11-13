#coding:utf-8
#
#   PROGRAM:     fdb
#   MODULE:      utils.py
#   DESCRIPTION: Various utility classes and functions
#   CREATED:     10.5.2013
#
#  Software distributed under the License is distributed AS IS,
#  WITHOUT WARRANTY OF ANY KIND, either express or implied.
#  See the License for the specific language governing rights
#  and limitations under the License.
#
#  The Original Code was created by Pavel Cisar
#
#  Copyright (c) 2013 Pavel Cisar <pcisar@users.sourceforge.net>
#  and all contributors signed below.
#
#  All Rights Reserved.
#  Contributor(s): ______________________________________.

def update_meta (self, other):
    "Helper function for :class:`LateBindingProperty` class."
    self.__name__ = other.__name__
    self.__doc__ = other.__doc__
    self.__dict__.update(other.__dict__)
    return self

class LateBindingProperty (property):
    """Peroperty class that binds to getter/setter/deleter methods when **instance**
of class that define the property is created. This allows you to override
these methods in descendant classes (if they are not private) without
necessity to redeclare the property itself in descendant class.

Recipe from Tim Delaney, 2005/03/31
http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/408713

::

    class C(object):

        def getx(self):
            print 'C.getx'
            return self._x

        def setx(self, x):
            print 'C.setx'
            self._x = x

        def delx(self):
            print 'C.delx'
            del self._x

        x = LateBindingProperty(getx, setx, delx)

    class D(C):

        def setx(self, x):
            print 'D.setx'
            super(D, self).setx(x)

        def delx(self):
            print 'D.delx'
            super(D, self).delx()

    c = C()
    c.x = 1
    c.x
    c.x
    del c.x

    print

    d = D()
    d.x = 1
    d.x
    d.x
    del d.x

This has the advantages that:

a. You get back an actual property object (with attendant memory savings, performance increases, etc);

b. It's the same syntax as using property(fget, fset, fdel, doc) except for the name;

c. It will fail earlier (when you define the class as opposed to when you use it).

d. It's shorter ;)

e. If you inspect the property you will get back functions with the correct __name__, __doc__, etc.

    """
    def __new__(self, fget=None, fset=None, fdel=None, doc=None):
        if fget is not None:
            def __get__(obj, objtype=None, name=fget.__name__):
                fget = getattr(obj, name)
                return fget()
            fget = update_meta(__get__, fget)
        if fset is not None:
            def __set__(obj, value, name=fset.__name__):
                fset = getattr(obj, name)
                return fset(value)
            fset = update_meta(__set__, fset)
        if fdel is not None:
            def __delete__(obj, name=fdel.__name__):
                fdel = getattr(obj, name)
                return fdel()
            fdel = update_meta(__delete__, fdel)
        return property(fget, fset, fdel, doc)

class Iterator(object):
    """Generic iterator implementation.
    """
    def __init__(self, method, sentinel = None):
        """
        :param method: Callable without parameters that returns next item.
        :param sentinel: Value that when returned by `method` indicates the end
                         of sequence.
        """
        self.getnext = method
        self.sentinel = sentinel
        self.exhausted = False
    def __iter__(self):
        return self
    def next(self):
        if self.exhausted:
            raise StopIteration
        else:
            result = self.getnext()
            self.exhausted = (result == self.sentinel)
            if self.exhausted:
                raise StopIteration
            else:
                return result
    __next__ = next

class EmbeddedProperty(property):
    """Property class that forwards calls to getter/setter/deleter methods to
respective property methods of another object. This class allows you to "inject"
properties from embedded object into class definition of parent object."""
    def __init__(self,obj,prop):
        """
        :param string obj: Attribute name with embedded object.
        :param property prop: Property instance from embedded object.
        """
        self.obj = obj
        self.prop = prop
        self.__doc__ = prop.__doc__
    def __get__(self,obj,objtype):
        if obj is None:
            return self
        return self.prop.__get__(getattr(obj,self.obj))
    def __set__(self,obj,val):
        self.prop.__set__(getattr(obj,self.obj),val)
    def __delete__(self,obj):
        self.prop.__delete__(getattr(obj,self.obj))

class EmbeddedAttribute(property):
    """Property class that gets/sets attribute of another object. This class
    allows you to "inject" attributes from embedded object into class definition
    of parent object."""
    def __init__(self,obj,attr):
        """
        :param string obj: Attribute name with embedded object.
        :param string attr: Attribute name from embedded object.
        """
        self.obj = obj
        self.attr = attr
        self.__doc__ = attr.__doc__
    def __get__(self,obj,objtype):
        if obj is None:
            return self
        return getattr(getattr(obj,self.obj),self.attr)
    def __set__(self,obj,val):
        setattr(getattr(obj,self.obj),self.attr,val)

def iter_class_properties(cls):
    """Iterator that yields `name, property` pairs for all properties in class.

    :param class cls: Class object."""
    for varname in vars(cls):
        value = getattr(cls, varname)
        if isinstance(value, property):
            yield varname, value

def iter_class_variables(cls):
    """Iterator that yields names of all non-callable attributes in class.

    :param class cls: Class object."""
    for varname in vars(cls):
        value = getattr(cls, varname)
        if not (isinstance(value, property) or callable(value)):
            yield varname

def embed_attributes(from_class,attr):
    """Class decorator that injects properties and non-callable attributes
from another class instance embedded in class instances.

    :param class from_class: Class that should extend decorated class.
    :param string attr: Attribute name that holds instance of embedded class
        within decorated class instance."""
    def d(class_):
        for pname,prop in iter_class_properties(from_class):
            if not hasattr(class_,pname):
                setattr(class_,pname,EmbeddedProperty(attr,prop))
        for attrname in iter_class_variables(from_class):
            if not hasattr(class_,attrname):
                setattr(class_,attrname,EmbeddedAttribute(attr,attrname))
        return class_
    return d

