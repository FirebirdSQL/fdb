#coding:utf-8
#
#   PROGRAM:     fdb
#   MODULE:      utils.py
#   DESCRIPTION: Python driver for Firebird - Various utility classes and functions
#   CREATED:     10.5.2013
#
#  Software distributed under the License is distributed AS IS,
#  WITHOUT WARRANTY OF ANY KIND, either express or implied.
#  See the License for the specific language governing rights
#  and limitations under the License.
#
#  The Original Code was created by Pavel Cisar
#
#  Copyright (c) Pavel Cisar <pcisar@users.sourceforge.net>
#  and all contributors signed below.
#
#  All Rights Reserved.
#  Contributor(s): ______________________________________.

from operator import attrgetter

def safe_int(str_value, base=10):
    """Always returns integer value from string/None argument. Returns 0 if argument is None.
"""
    if str_value:
        return int(str_value, base)
    else:
        return 0

def safe_str(str_value):
    """Always returns string value from string/None argument.
Returns empty string if argument is None.
"""
    if str_value is None:
        return ''
    else:
        return str_value

def update_meta(self, other):
    "Helper function for :class:`LateBindingProperty` class."
    self.__name__ = other.__name__
    self.__doc__ = other.__doc__
    self.__dict__.update(other.__dict__)
    return self

class LateBindingProperty(property):
    """Property class that binds to getter/setter/deleter methods when **instance**
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
    def __new__(cls, fget=None, fset=None, fdel=None, doc=None):
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
    def __init__(self, method, sentinel=None):
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
    def __init__(self, obj, prop):
        """
        :param string obj: Attribute name with embedded object.
        :param property prop: Property instance from embedded object.
        """
        self.obj = obj
        self.prop = prop
        self.__doc__ = prop.__doc__
    def __get__(self, obj, objtype):
        if obj is None:
            return self
        return self.prop.__get__(getattr(obj, self.obj))
    def __set__(self, obj, val):
        self.prop.__set__(getattr(obj, self.obj), val)
    def __delete__(self, obj):
        self.prop.__delete__(getattr(obj, self.obj))

class EmbeddedAttribute(property):
    """Property class that gets/sets attribute of another object. This class
    allows you to "inject" attributes from embedded object into class definition
    of parent object."""
    def __init__(self, obj, attr):
        """
        :param string obj: Attribute name with embedded object.
        :param string attr: Attribute name from embedded object.
        """
        self.obj = obj
        self.attr = attr
        self.__doc__ = attr.__doc__
    def __get__(self, obj, objtype):
        if obj is None:
            return self
        return getattr(getattr(obj, self.obj), self.attr)
    def __set__(self, obj, val):
        setattr(getattr(obj, self.obj), self.attr, val)

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
        if not (isinstance(value, property) or callable(value)) and not varname.startswith('_'):
            yield varname

def embed_attributes(from_class, attr):
    """Class decorator that injects properties and attributes
from another class instance embedded in class instances. Only attributes and properties that are not
already defined in decorated class are injected.

    :param class from_class: Class that should extend decorated class.
    :param string attr: Attribute name that holds instance of embedded class
        within decorated class instance."""
    def d(class_):
        for pname, prop in iter_class_properties(from_class):
            if not hasattr(class_, pname):
                setattr(class_, pname, EmbeddedProperty(attr, prop))
        for attrname in iter_class_variables(from_class):
            if not hasattr(class_, attrname):
                setattr(class_, attrname, EmbeddedAttribute(attr, attrname))
        return class_
    return d

def make_lambda(expr, params='item', context=None):
    """Make lambda function from expression.

    .. versionadded:: 2.0
"""
    if context:
        return eval('lambda %s:%s' % (params, expr), context)
    else:
        return eval('lambda %s:%s' % (params, expr))

class ObjectList(list):
    """List of objects with additional functionality.

    .. versionadded:: 2.0
"""
    def __init__(self, items=None, _cls=None, key_expr=None):
        """
            :param iterable items: Sequence to initialize the collection.
            :param _cls: Class or list/tuple of classes. Only instances of these classes would be allowed in collection.
            :param str key_expr: Key expression. Must contain item referrence as `item`, for example `item.attribute_name`.

            :raises ValueError: When initialization sequence contains invalid instance.
            """
        if items:
            super(ObjectList, self).__init__(items)
        else:
            super(ObjectList, self).__init__()
        self.__key_expr = key_expr
        self.__frozen = False
        self._cls = _cls
        self.__map = None
    def __check_value(self, value):
        if self._cls and not isinstance(value, self._cls):
            raise TypeError("Value is not an instance of allowed class")
    def __check_mutability(self):
        if self.__frozen:
            raise TypeError("list is frozen")
    def __setitem__(self, index, value):
        self.__check_mutability()
        self.__check_value(value)
        super(ObjectList, self).__setitem__(index, value)
    def __setslice__(self, i, j, y):
        self.__check_mutability()
        super(ObjectList, self).__setslice__(i, j, y)
    def __delitem__(self, index):
        self.__check_mutability()
        super(ObjectList, self).__delitem__(index)
    def __delslice__(self, i, j):
        self.__check_mutability()
        super(ObjectList, self).__delslice__(i, j)
    def insert(self, index, item):
        """Insert item before index.

        :raises TypeError: When list is frozen or item is not an instance of allowed class"""
        self.__check_mutability()
        self.__check_value(item)
        super(ObjectList, self).insert(index, item)
    def append(self, item):
        """Add an item to the end of the list.

        :raises TypeError: When list is frozen or item is not an instance of allowed class"""
        self.__check_mutability()
        self.__check_value(item)
        super(ObjectList, self).append(item)
    def extend(self, iterable):
        """Extend the list by appending all the items in the given iterable.

        :raises TypeError: When list is frozen or item is not an instance of allowed class"""
        for item in iterable:
            self.append(item)
    def sort(self, attrs=None, expr=None, reverse=False):
        """Sort items in-place, optionaly using attribute values as key or key expression.

        :param list attrs: List of attribute names.
        :param expr:       Key expression, a callable accepting one parameter or expression as string referencing list item as `item`.

        .. important::

           Only one parameter (`attrs` or `expr`) could be specified. If none is present then uses default list sorting rule.

        :raises TypeError: When list is frozen.

        Examples::

            sort(attrs=['name','degree'])       # Sort by item.name, item.degree
            sort(expr=lambda x: x.name.upper()) # Sort by upper item.name
            sort(expr='item.name.upper()')      # Sort by upper item.name
    """
        self.__check_mutability()
        if attrs:
            super(ObjectList, self).sort(key=attrgetter(*attrs), reverse=reverse)
        elif expr:
            super(ObjectList, self).sort(key=expr if callable(expr) else make_lambda(expr), reverse=reverse)
        else:
            super(ObjectList, self).sort(reverse=reverse)
    def reverse(self):
        """Reverse the elements of the list, in place.

        :raises TypeError: When list is frozen."""
        self.__check_mutability()
        super(ObjectList, self).reverse()
    def clear(self):
        """Remove all items from the list.

        :raises TypeError: When list is frozen."""
        self.__check_mutability()
        while len(self) > 0:
            del self[0]
    def freeze(self):
        """Set list to immutable (frozen) state."""
        self.__frozen = True
        if self.__key_expr:
            fce = make_lambda(self.__key_expr)
            self.__map = dict(((key, index) for index, key in enumerate((fce(item) for item in self))))
    def filter(self, expr):
        """Return new ObjectList of items for which `expr` is evaluated as True.

        :param expr:   Boolean expression, a callable accepting one parameter or expression as string referencing list item as `item`.

        Example::

            filter(lambda x: x.name.startswith("ABC"))
            filter('item.name.startswith("ABC")')
"""
        fce = expr if callable(expr) else make_lambda(expr)
        return ObjectList(self.ifilter(expr), self._cls, self.__key_expr)
    def ifilter(self, expr):
        """Return generator that yields items for which `expr` is evaluated as True.

        :param expr:   Boolean expression, a callable accepting one parameter or expression as string referencing list item as `item`.

        Example::

            ifilter(lambda x: x.name.startswith("ABC"))
            ifilter('item.name.startswith("ABC")')
"""
        fce = expr if callable(expr) else make_lambda(expr)
        return (item for item in self if fce(item))
    def ifilterfalse(self, expr):
        """Return generator that yields items for which `expr` is evaluated as False.

        :param expr:   Boolean expression, a callable accepting one parameter or expression as string referencing list item as `item`.

        Example::

            ifilter(lambda x: x.name.startswith("ABC"))
            ifilter('item.name.startswith("ABC")')
"""
        fce = expr if callable(expr) else make_lambda(expr)
        return (item for item in self if not fce(item))
    def report(self, *args):
        """Return list of data produced by expression(s) evaluated on list items.

        Parameter(s) could be one from:

        - A callable accepting one parameter and returning data for output
        - One or more expressions as string referencing item as `item`.

        Examples::

            # returns list of tuples with item.name and item.size

            report(lambda x: (x.name, x.size))
            report('item.name','item.size')

            # returns list of item names

            report(lambda x: x.name)
            report('item.name')
"""
        if len(args) == 1 and callable(args[0]):
            fce = args[0]
        else:
            attrs = "(%s)" % ",".join(args) if len(args) > 1 else args[0]
            fce = make_lambda(attrs)
        return [fce(item) for item in self]
    def ireport(self, *args):
        """Return generator that yields data produced by expression(s) evaluated on list items.

        Parameter(s) could be one from:

        - A callable accepting one parameter and returning data for output
        - One or more expressions as string referencing item as `item`.

        Examples::

            # generator of tuples with item.name and item.size

            report(lambda x: (x.name, x.size))
            report('item.name','item.size')

            # generator of item names

            report(lambda x: x.name)
            report('item.name')
"""
        if len(args) == 1 and callable(args[0]):
            fce = args[0]
        else:
            attrs = "(%s)" % ",".join(args) if len(args) > 1 else args[0]
            fce = make_lambda(attrs)
        return (fce(item) for item in self)
    def ecount(self, expr):
        """Return number of items for which `expr` is evaluated as True.

        :param expr:   Boolean expression, a callable accepting one parameter or expression as string referencing list item as `item`.

        Example::

            ecount(lambda x: x.name.startswith("ABC"))
            ecount('item.name.startswith("ABC")')
"""
        return sum(1 for item in self.ifilter(expr))
    def split(self, expr):
        """Return two new ObjectLists, first with items for which `expr` is evaluated as True and second for `expr` evaluated as False.

        :param expr:   Boolean expression, a callable accepting one parameter or expression as string referencing list item as `item`.

        Example::

            split(lambda x: x.size > 100)
            split('item.size > 100')
"""
        return ObjectList(self.ifilter(expr), self._cls, self.__key_expr), ObjectList(self.ifilterfalse(expr), self._cls, self.__key_expr)
    def extract(self, expr):
        """Move items for which `expr` is evaluated as True into new ObjectLists.

        :param expr:   Boolean expression, a callable accepting one parameter or expression as string referencing list item as `item`.

        :raises TypeError: When list is frozen.

        Example::

            extract(lambda x: x.name.startswith("ABC"))
            extract('item.name.startswith("ABC")')
"""
        self.__check_mutability()
        fce = expr if callable(expr) else make_lambda(expr)
        l = ObjectList(_cls=self._cls, key_expr=self.__key_expr)
        i = 0
        while len(self) > i:
            item = self[i]
            if fce(item):
                l.append(item)
                del self[i]
            else:
                i += 1
        return l
    def get(self, value, expr=None):
        """Return item with given key value using default or specified key expression, or None if there is no such item.

        Uses very fast method to look up value of default key expression in `frozen` list, otherwise it uses slower list traversal.

        :param value:  Searched value.
        :param expr:   Key value expression, a callable accepting two parameters (item,value) or expression as string referencing list item as `item`.

        :raises TypeError: If key expression is not defined.

        Examples::

            # Search using default key expression
            get('ITEM_NAME')
            # Search using callable key expression
            get('ITEM_NAME',lambda x: x.name.upper())
            # Search using string key expression
            get('ITEM_NAME','item.name.upper()')
"""
        if self.__map and not expr:
            i = self.__map.get(value)
            return self[i] if i is not None else None
        if not (self.__key_expr or expr):
            raise TypeError("Key expression required")
        if callable(expr):
            fce = expr
        else:
            s = '%s == value' % (self.__key_expr if expr is None else expr)
            fce = make_lambda(s, 'item,value')
        for item in self:
            if fce(item, value):
                return item
        return None
    def contains(self, value, expr=None):
        """Return True if list has any item with default or specified key expression equal to given value.

        :param value:  Tested key value.
        :param expr:   Key value expression, a callable accepting two parameters (item,value) or expression as string referencing list item as `item`.

        Examples::

            # Search using default key expression
            contains('ITEM_NAME')
            # Search using callable key expression
            contains('ITEM_NAME',lambda x: x.name.upper())
            # Search using string key expression
            contains('ITEM_NAME','item.name.upper()')
"""
        return False if self.get(value, expr) is None else True
    def all(self, expr):
        """Return True if `expr` is evaluated as True for all list elements.

        :param expr:   Boolean expression, a callable accepting one parameter or expression as string referencing list item as `item`.

        Example::

            all(lambda x: x.name.startswith("ABC"))
            all('item.name.startswith("ABC")')
"""
        fce = expr if callable(expr) else make_lambda(expr)
        for item in self:
            if not fce(item):
                return False
        return True
    def any(self, expr):
        """Return True if `expr` is evaluated as True for any list element.

        :param expr:   Boolean expression, a callable accepting one parameter or expression as string referencing list item as `item`.

        Example::

            any(lambda x: x.name.startswith("ABC"))
            any('item.name.startswith("ABC")')
"""
        fce = expr if callable(expr) else make_lambda(expr)
        for item in self:
            if not fce(item):
                return True
        return False
    #
    frozen = property(fget=lambda self: self.__frozen, doc='True if list is immutable')
    key = property(fget=lambda self: self.__key_expr, doc='Key expression')

class Visitable(object):
    """Base class for Visitor Pattern support.

    .. versionadded:: 2.0
"""
    def accept(self, visitor):
        """Visitor Pattern support. Calls `visit(self)` on parameter object.

        :param visitor: Visitor object of Visitor Pattern.
        """
        visitor.visit(self)

class Visitor(object):
    """Base class for Visitor Pattern visitors.

    .. versionadded:: 2.0

    Descendants may implement methods to handle individual object types that follow naming pattern `visit_<class_name>`.
    Calls :meth:`default_action` if appropriate special method is not defined.

    .. important::

       This implementation uses Python Method Resolution Order (__mro__) to find special handling method, so special
       method for given class is used also for its decendants.

    Example::

       class Node(object): pass
       class A(Node): pass
       class B(Node): pass
       class C(A,B): pass

       class MyVisitor(object):
           def default_action(self, obj):
               print 'default_action '+obj.__class__.__name__

           def visit_B(self, obj):
               print 'visit_B '+obj.__class__.__name__


       a = A()
       b = B()
       c = C()
       visitor = MyVisitor()
       visitor.visit(a)
       visitor.visit(b)
       visitor.visit(c)

    Will create output::

       default_action A
       visit_B B
       visit_B C
"""
    def visit(self, obj):
        """Dispatch to method that handles `obj`.

        First traverses the `obj.__mro__` to try find method with name following `visit_<class_name>` pattern and calls it with
        `obj`. Otherwise it calls :meth:`default_action`.

        :param object obj: Object to be handled by visitor.
"""
        meth = None
        for cls in obj.__class__.__mro__:
            meth = getattr(self, 'visit_'+cls.__name__, None)
            if meth:
                break
        if not meth:
            meth = self.default_action
        return meth(obj)
    def default_action(self, obj):
        """Default handler for visited objects.

        :param object obj: Object to be handled.

        .. note:: This implementation does nothing!
"""
        pass
