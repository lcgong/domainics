
from domainics.sqltext import SQL

def test_sqltext():
    name, age = "abc", 28

    s = SQL("[{name},{age},{age + 10}];")
    s += SQL(f"<{{name}},{age}>")
    assert len(s._segments) == 10

    stmt, vals = s.get_statment()
    print(stmt, vals)
    assert stmt == '[$1,$2,$3];<$4,28>'
    assert vals == ['abc', 28, 38, 'abc']

    s0 = SQL('[')
    s1  = s0 + '{age}' + ']'
    stmt, vals = s1.get_statment()
    assert stmt=='[$1]' and vals == [28]
    stmt, vals = s0.get_statment()
    assert stmt=='[' and vals == []

    s0 = SQL('[')
    s0 += '{age}' + ']'
    stmt, vals = s0.get_statment()
    assert stmt=='[$1]' and vals == [28]
