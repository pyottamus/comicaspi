from cpython.ref cimport PyObject
from cython.operator import dereference
cdef extern from "datetime.h":
	struct PyDateTime_Date:
		pass
	
	int PyDate_Check(PyObject*);
	cdef struct PyDateTime_Date:
		unsigned char data[4];
	
	ctypedef class datetime.date [object PyDateTime_Date]:
		cdef PyDateTime_Date cval
		


cdef extern from "dateutil_ext.h":
	unsigned int _extract(PyObject* obj);
	PyObject* _de_extract(unsigned int);
	unsigned long long _gen_key(PyObject*, unsigned int);
	void _init();
	
def serialize_date(date date_val):
	return _extract(<PyObject *>date_val)

def deserialize_date(unsigned int val):
	ret = _de_extract(val)
	if ret==NULL:
		raise
	return <date>ret

def is_leap(unsigned int year):
	"""year -> 1 if leap year, else 0."""
	#/* Cast year to unsigned.  The result is the same either way, but
	# * C can generate faster code for unsigned mod than for signed
	# * mod (especially for % 4 -- a good compiler should just grab
	# * the last 2 bits when the LHS is unsigned).
	# */
	return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

def gen_key(date date_val, unsigned int secondary):
	return _gen_key(<PyObject *>date_val, secondary)

def gen_key_serialize(date date_val, unsigned int secondary):
	return _gen_key(<PyObject *>date_val, secondary), _extract(<PyObject*>date_val)
_init()
