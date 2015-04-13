#!/usr/bin/env python
# coding=utf-8
# Copyright (C) 2015 Wesley Tanaka
"""Experiments with avro
"""

import json
import StringIO

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from avro.io import AvroTypeException

def main():
   known_schemas = avro.schema.Names()

   with open("point.avsc", "rb") as fp:
      point = avro.schema.make_avsc_object(json.loads(fp.read()), known_schemas)

   with open("review.avsc", "rb") as fp:
      place = avro.schema.make_avsc_object(json.loads(fp.read()), known_schemas)

   with open("place.avsc", "rb") as fp:
      place = avro.schema.make_avsc_object(json.loads(fp.read()), known_schemas)

   output = StringIO.StringIO()
   writer = DataFileWriter(output, DatumWriter(), point)
   writer.append({'x': 1.5, 'y': 2.75})
   writer.flush()
   serialized = output.getvalue()
   reader = DataFileReader(StringIO.StringIO(serialized), DatumReader())
   deserialized = tuple(reader)[0]
   assert deserialized['x'] == 1.5
   assert deserialized['y'] == 2.75
   reader.close()
   writer.close()

   try:
      output = StringIO.StringIO()
      writer = DataFileWriter(output, DatumWriter(), point)
      writer.append({'x': 1.5})
      assert False
   except AvroTypeException as e:
      pass

   try:
      output = StringIO.StringIO()
      writer = DataFileWriter(output, DatumWriter(), point)
      writer.append({'x': 1.5, 'y': "wtanaka.com"})
      assert False
   except AvroTypeException as e:
      pass

   output = StringIO.StringIO()
   writer = DataFileWriter(output, DatumWriter(), place)
   writer.append({
         'name': 'wtanaka.com',
         'location': {'x': 1.5, 'y': 2.75}
         })
   writer.flush()
   serialized = output.getvalue()
   reader = DataFileReader(StringIO.StringIO(serialized), DatumReader())
   deserialized = tuple(reader)[0]
   assert deserialized['location']['x'] == 1.5
   assert deserialized['location']['y'] == 2.75
   reader.close()
   writer.close()

   output = StringIO.StringIO()
   writer = DataFileWriter(output, DatumWriter(), place)
   writer.append({
         'name': 'wtanaka.com',
         'location': {'x': 1.5, 'y': 2.75},
         'review': {'rating': 4, 'text': '4 stars would come again'},
         })
   writer.flush()
   serialized = output.getvalue()
   reader = DataFileReader(StringIO.StringIO(serialized), DatumReader())
   deserialized = tuple(reader)[0]
   assert deserialized['location']['x'] == 1.5
   assert deserialized['location']['y'] == 2.75
   reader.close()
   writer.close()

   try:
      output = StringIO.StringIO()
      writer = DataFileWriter(output, DatumWriter(), place)
      writer.append({
            'name': 'wtanaka.com',
            'location': {'x': 1.5, 'y': 2.75},
            'review': {'x': 1.5, 'y': 2.75},
            })
      assert False
   except AvroTypeException as e:
      pass

if __name__ == "__main__":
   main()
