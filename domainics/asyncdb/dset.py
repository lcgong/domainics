


    def __dset__(self, item_type):

        dobj_attrs = OrderedDict((attr_name, attr) for attr_name, attr in
                            iter_chain(item_type.__dobject_key__.items(),
                                       item_type.__dobject_att__.items()))

        colnames = []
        selected = []
        for i, attr_name in enumerate(self._attr_names):
            colname = d[0]
            if colname in dobj_attrs:
                selected.append(i)
                colnames.append(attr_name)

        records = self.records
        for i in range(self._n_records):
            row = records[0]

            obj = dict((k, v)
                       for k, v in zip(colnames, (record[i] for i in selected)))
            yield item_type(**obj)
