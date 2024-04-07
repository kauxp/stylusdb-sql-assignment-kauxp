const executeSELECTQuery= require('../../src/index');
const parseQuery = require('../../src/queryParser');

test('Execute SQL Query with LEFT JOIN', async () => {
    const query = 'SELECT student.name, enrollment.course FROM student LEFT JOIN enrollment ON student.id=enrollment.student_id';
    const result = await executeSELECTQuery(query);
    expect(result).toEqual(expect.arrayContaining([
        expect.objectContaining({ "student.name": "Alice", "enrollment.course": null }),
        expect.objectContaining({ "student.name": "John", "enrollment.course": "Mathematics" })
    ]));
    expect(result.length).toEqual(5); // 4 students, but John appears twice
});

test('Execute SQL Query with RIGHT JOIN', async () => {
    const query = 'SELECT student.name, enrollment.course FROM student RIGHT JOIN enrollment ON student.id=enrollment.student_id';
    const result = await executeSELECTQuery(query);
    expect(result).toEqual(expect.arrayContaining([
        expect.objectContaining({ "student.name": "Bob", "enrollment.course": null }),
        expect.objectContaining({ "student.name": "John", "enrollment.course": null })
    ]));
    expect(result.length).toEqual(3); // 4 students, but John appears twice
});