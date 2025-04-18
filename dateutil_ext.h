#ifndef DATEUTIL_CEXT_H
#define DATEUTIL_CEXT_H
#include <datetime.h>

#define _init() PyDateTime_IMPORT

static inline unsigned int _extract(PyObject* obj){
	return _byteswap_ulong(*((unsigned int*)(((PyDateTime_Date*)obj)->data)));
}

static inline __forceinline unsigned long long _gen_key(PyObject* date_obj, unsigned int secondary){
	return (((unsigned long long)(_extract(date_obj))) << 32ull) | secondary;
}

static inline PyObject* _de_extract(unsigned int val){
	PyDateTime_Date* ret;
	
	ret = _PyObject_New(PyDateTimeAPI->DateType);
	if(ret==NULL){
		return NULL;
	}
	
	val = _byteswap_ulong(val);
	*((unsigned int*)ret->data) = val;
	return ret;
}
#endif /* !DATEUTIL_CEXT_H */