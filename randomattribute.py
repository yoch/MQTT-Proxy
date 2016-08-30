#!/usr/bin/env python
# -*- coding: utf8 -*-

import random
import itertools
from functools import partial


class Charset:
    def __init__(self, min, max):
        self.rng = range(min, max + 1)

    def __len__(self):
        return len(self.rng)

    def __getitem__(self, i):
        return chr(self.rng[i])


class RandomAttr:
    def __init__(self):
        self.__choices = {}
        self._label = None

    def __get__(self, instance, owner):
        if instance is None:
            return self

        if not instance._random:
            return instance.__dict__[self._label]

        choices = self._choices(instance)
        val = random.choice(choices)
        print('\tAttr', self._label, val)
        return val

    def __set__(self, instance, value):
        instance.__dict__[self._label] = value


class Integer(RandomAttr):
    def __init__(self, range):
        super().__init__()
        self.range = range

    def _choices(self, instance):
        choices = [self.range.start, self.range.stop]
        value = instance.__dict__.get(self._label)
        if value is not None:
            if value > self.range.start:
                choices.append(value - 1)
            if value < self.range.stop:
                choices.append(value + 1)
        return choices

class Text(RandomAttr):
    def __init__(self, range, charset):
        super().__init__()
        self.range = range
        self.charset = charset

    def _choices(self, instance):
        v1 = ''.join(random.choice(self.charset) for _ in range(self.range.start))
        v2 = ''.join(random.choice(self.charset) for _ in range(self.range.start + 1))
        v3 = ''.join(random.choice(self.charset) for _ in range(self.range.stop - 1))
        v4 = ''.join(random.choice(self.charset) for _ in range(self.range.stop))
        choices = [v1, v2, v3, v4]
        value = instance.__dict__.get(self._label)
        if value is not None:
            if len(value) > self.range.start:
                choices.append(value[:-1])
            if len(value) < self.range.stop:
                choices.append(value + random.choice(self.charset))
        return choices

class Binary(RandomAttr):
    def __init__(self):
        super().__init__()

    def _choices(self, instance):
        #TODO
        choices = [b'']
        value = instance.__dict__.get(self._label)
        if value is not None:
            if len(value) > self.range.start:
                choices.append(value[:-1])
            if len(value) < self.range.stop:
                choices.append(value + bytes([random.randrange(256)]))
        return choices

class Bit(RandomAttr):
    def _choices(self, instance):
        return [0, 1]

# class Bitset(RandomAttr):
    # def __init__(self, size):
        # super().__init__()
        # self.size = size
    # def _choices(self):
        # v1 = tuple(0 for _ in range(self.size))
        # v2 = tuple(1 for _ in range(self.size))
        # v3 = tuple(random.choice([0,1]) for _ in range(self.size))
        # choices = [v1, v2, v3]
        # if value is not None:
            # for _ in range(self.size):
                # v = value[:]
                # v[i] = 1 - v[i]
                # choices.append(v)
        # return choices

class List(RandomAttr):
    def __init__(self, type):
        super().__init__()
        self._type = type

    def _choices(self, instance):
        choices = [[]]
        value = instance.__dict__.get(self._label)
        if value is not None:
            lst = [elt.__get__(instance, type(instance)) for elt in self._lst]
            choices.append(lst)
        return choices

    def __set__(self, instance, value):
        assert isinstance(value, list)
        super().__set__(instance, value)
        self._lst = [self._type() for _ in range(len(value))]
        for i, elt in enumerate(self._lst):
            elt._label = self._label + '-' + str(i)
            elt.__set__(instance, value[i])
        
        

Uint8 = partial(Integer, range=range(0, 0xff))
Uint16 = partial(Integer, range=range(0, 0xffff))
Ascii = partial(Text, charset=Charset(0, 0xff))
Utf8 = partial(Text, charset=Charset(0, 0xd7ff), range=range(0,0xffff))   # partiel, a cause des trous
