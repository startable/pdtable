Writing
=======

Use the aptly named ``write_csv()`` and ``write_excel()`` to write to files as well as to text streams.

Let’s write the tables we read earlier to a text stream.

>>> from pdtable import write_csv 
>>> with StringIO() as s:
...     write_csv(tables, s)
...     print(s.getvalue())
...
**places;
all
place;distance;ETA;is_hot
text;km;datetime;onoff
home;0.0;2020-08-04 08:00:00;1
work;1.0;2020-08-04 09:00:00;0
beach;2.0;2020-08-04 17:00:00;1
\n
**farm_animals;
my_farm your_farm other_farm
species;n_legs;avg_weight
text;-;kg
chicken;2.0;2.0
pig;4.0;89.0
cow;4.0;200.0
unicorn;4.0;-