from hatchet import GraphFrame

def nodewise_metric_agg(gf, agg_index_level, agg_func_dict): #, append_gf=None):
    if agg_index_level == "node":
        raise ValueError("Cannot aggregate over index level 'node'")
    if agg_index_level not in gf.dataframe.index.names:
        raise KeyError("'{}' not in index of GraphFrame".format(agg_index_level))
    if target_metric not in gf.dataframe.columns:
        raise KeyError("Metric '{}' not in GraphFrame".format(target_metric))
    index_names = list(gf.dataframe.index.names)
    index_names.remove(agg_index_level)
    df = gf.dataframe.reset_index()
    new_nodewise_df = []
    new_exc = []
    new_inc = []
    for group_name, node_subdf in df.groupby(by=index_names, sort=False):
        nodewise_row = {}
        if len(index_names) > 1:
            for i in range(len(index_names)):
                nodewise_row[index_names[i]] = group_name[i]
        else:
            nodewise_row[index_names[0]] = group_name
        local_node_subdf = node_subdf.reset_index()
        for metric, agg_func in agg_func_dict.items():
            target_metric_vals = local_node_subdf[metric].tolist()
            new_metrics = agg_func(target_metric_vals, metric)
            nodewise_row.update(new_metrics)
            for key in nodewise_row:
                if key in gf.exc_metrics:
                    new_exc.append(key)
                elif key in gf.inc_metrics:
                    new_inc.append(key)
            for key in new_metrics:
                if key in new_exc:
                    new_exc.remove(key)
                elif key in new_inc:
                    new_inc.remove(key)
            if target_metric in gf.inc_metrics:
                new_inc.extend(new_metrics.keys())
            else:
                new_exc.extend(new_metrics.keys())
    new_df = pd.DataFrame(data=new_nodewise_df)
    new_df.set_index(index_names, inplace=True)
    new_gf = GraphFrame(
        gf.graph.copy(),
        new_df,
        exc_metrics=new_exc,
        inc_metrics=new_inc,
    )
    return new_gf
