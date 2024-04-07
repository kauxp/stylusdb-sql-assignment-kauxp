const executeSELECTQuery= require('../../src/index');

const parseQuery = require('../../src/queryParser');
test('Parse basic GROUP BY query', () => {
    const query = 'SELECT age, COUNT(*) FROM student GROUP BY age';
    const parsed = parseQuery(query);
    expect(parsed).toEqual({
        fields: ['age', 'COUNT(*)'],
        table: 'student',
        whereClauses: [],
        groupByFields: ['age'],
        joinType: null,
        joinTable: null,
        joinCondition: null,
        hasAggregateWithoutGroupBy: true
    });
});

test('Count students per age', async () => {
    const query = 'SELECT age, COUNT(*) FROM student GROUP BY age';
    const result = await executeSELECTQuery(query);
    console.log(result)
    expect(result).toEqual([
        { age: '22', 'COUNT(*)': 1 },
        { age: '24', 'COUNT(*)': 1 },
        { age: '25', 'COUNT(*)': 1 },
        { age: '30', 'COUNT(*)': 1 }
    ]);
});

// test('Count enrollments per course', async () => {
//     const query = 'SELECT course, COUNT(*) FROM enrollment GROUP BY course';
//     const result = await executeSELECTQuery(query);
//     expect(result).toEqual([
//         { course: 'Mathematics', 'COUNT(*)': 2 },
//         { course: 'Physics', 'COUNT(*)': 1 },
//         { course: 'Chemistry', 'COUNT(*)': 1 },
//         { course: 'Biology', 'COUNT(*)': 1 }
//     ]);
// });

// test('Count courses per student', async () => {
//     const query = 'SELECT student_id, COUNT(*) FROM enrollment GROUP BY student_id';
//     const result = await executeSELECTQuery(query);
//     expect(result).toEqual([
//         { student_id: '1', 'COUNT(*)': 2 },
//         { student_id: '2', 'COUNT(*)': 1 },
//         { student_id: '3', 'COUNT(*)': 1 },
//         { student_id: '5', 'COUNT(*)': 1 }
//     ]);
// });

// test('Count students within a specific age range', async () => {
//     const query = 'SELECT age, COUNT(*) FROM student WHERE age > 22 GROUP BY age';
//     const result = await executeSELECTQuery(query);
//     expect(result).toEqual([
//         { age: '24', 'COUNT(*)': 1 },
//         { age: '25', 'COUNT(*)': 1 },
//         { age: '30', 'COUNT(*)': 1 }
//     ]);
// });

// test('Count enrollments for a specific course', async () => {
//     const query = 'SELECT course, COUNT(*) FROM enrollment WHERE course = "Mathematics" GROUP BY course';
//     const result = await executeSELECTQuery(query);
//     expect(result).toEqual([
//         { course: 'Mathematics', 'COUNT(*)': 2 }
//     ]);
// });