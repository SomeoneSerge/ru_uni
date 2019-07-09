function mapf() {
    function classif1(row) {
        for (cell in row.values) {
            if (cell.colname.startsWith('Код классификатора')) {
                return cell.value == '1';
            }
        }
        emit('fuck', row.name);
        return false;
    }
    // if (this.section != '2.1.3') { return }
    for(row in this.rows) {
        if (classif1(row)) {
            for (cell in row.values) {
                if(cell.colname.startsWith('Выпуск фактический')) {
                    emit(row.name, cell.value)
                }
            }
        }
    }
}
function reducef(key, values) {
    return values.length;
}
db.vpo1.mapReduce(mapf, reducef, out='graduates');
