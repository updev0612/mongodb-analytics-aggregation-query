class Query {

    constructor(collection) {
        this._collection = collection;
        this._fields = [];
        this._groups = [];
        this._filter = {};
        this._constraints = {};
    }

    select(fields) {
        this._fields = fields;
        return this;
    }

    groupBy(groups) {
        this._groups = groups;
        return this;
    }

    where(filter) {
        this._filter = filter;
        return this;
    }

    having(constraints) {
        this._constraints = constraints;
        return this;
    }

    execute() {
        const pipeline = [
            this.filterStage,
            this.sumStage,
            this.groupStage,
            this.flattenStage,
            this.constraintsStage,
        ];

        return this._collection.aggregate(pipeline).toArray();
    }

    get filterStage() {
        return {
            $match: {
                name: {$in: this._fields},
                ...this._filter,
            }
        };
    }

    get sumStage() {
        return {
            $group: {
                _id: this._getGroupId(this._groups.concat('name'), '$'),
                sum: {$sum: 1}
            }
        };
    }

    get groupStage() {
        return {
            $group: {
                _id: this._getGroupId(this._groups, '$_id.'),
                metrics: {$push: {k: "$_id.name", v: "$sum"}}
            }
        };
    }

    get flattenStage() {
        return {
            $replaceRoot: {
                newRoot: {
                    $mergeObjects: [
                        this._getGroupId(this._groups, '$_id.'),
                        {$arrayToObject: "$metrics"}
                    ]
                }
            }
        };
    }

    get constraintsStage() {
        return {
            $match: this._constraints
        };
    }

    _getGroupId(dimensions, prefix) {
        const id = {};
        for (let d of dimensions) {
            id[d] = prefix + d;
        }
        return id;
    }

}

(async function () {
    const client = new MongoClient(URL);
    const connection = await client.connect();
    const db = connection.db('analytics');
    const collection = db.collection('events');

    const query = new Query(collection);

    query
        .select(['click', 'start'])
        .groupBy(['country', 'browser'])
        .where({browser: {$in: ['safari', 'chrome']}})
        .having({click: {$gt: 7200}, start: {$gt: 7200}});

    const report = await query.execute();
})();
