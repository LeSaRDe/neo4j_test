import logging
import snap


def TNEANet_graph_creation_test():
    """
    As described in the official doc of SNAP, TNEANet is a directed multigraph supporting node and edge attributes.
    Lets create a TNEANet instance with some node and edge attributes.
    NOTE:
        Adding a node takes constant time, while adding an edge takes linear time in the node degree as adjacency list
        is kept sorted.
        Accessing arbitrary node takes constant time and accessing any edge takes logarithmic time in node degree.
    TODO:
        Do we need to call Defrag() after the graph construction?
    :return: snap.TNEANet
    """
    # CREATE GRAPH INSTANCE
    tneanet_ins = snap.TNEANet.New()

    # ADD EMPTY NODE ATTRIBUTES
    # NOTE:
    #   Notice that empty attributes can be added before nodes and edges are added into graph. Also, this step is NOT
    #   necessary for adding attributes to nodes and edges.
    tneanet_ins.AddStrAttrN('E_ATT')
    tneanet_ins.AddIntAttrE('E_ATT')

    # ADD NODES
    for i in range(5):
        tneanet_ins.AddNode(i)

    # ADD EDGES
    for ni in tneanet_ins.Nodes():
        ni_id = ni.GetId()
        for nj in tneanet_ins.Nodes():
            nj_id = nj.GetId()
            if ni_id == nj_id:
                continue
            tneanet_ins.AddEdge(ni_id, nj_id)

    # ADD NODE ATTRIBUTES
    for ni in tneanet_ins.Nodes():
        ni_id = ni.GetId()
        tneanet_ins.AddIntAttrDatN(ni_id, ni_id, 'PID')
        tneanet_ins.AddFltAttrDatN(ni_id, ni_id / 10.0, 'F_ATT')
        tneanet_ins.AddStrAttrDatN(ni_id, '#' + str(ni_id), 'S_ATT')

    # ADD EDGE ATTRIBUTES
    for idx, ei in enumerate(tneanet_ins.Edges()):
        tneanet_ins.AddIntAttrDatE(ei, (idx + 1) * 100, 'DURATION')

    return tneanet_ins


def TNEANet_test_in_memory(tneanet_ins):
    # tneanet_ins = TNEANet_graph_creation_test()
    for ni in tneanet_ins.Nodes():
        ni_id = ni.GetId()
        pid = tneanet_ins.GetIntAttrDatN(ni_id, 'PID')
        f_att = tneanet_ins.GetFltAttrDatN(ni_id, 'F_ATT')
        s_att = tneanet_ins.GetStrAttrDatN(ni_id, 'S_ATT')
        e_att = tneanet_ins.GetStrAttrDatN(ni_id, 'E_ATT')
        print('Node %s: PID=%s, f_att=%s, s_att=%s, s_att_2=%s' % (ni_id, pid, f_att, s_att, e_att))
    for ei in tneanet_ins.Edges():
        ei_id = ei.GetId()
        ei_src_id = ei.GetSrcNId()
        ei_trg_id = ei.GetDstNId()
        duration = tneanet_ins.GetIntAttrDatE(ei_id, 'DURATION')
        print('Edge %s: src_id=%s, dst_id=%s, DURATION=%s' % (ei_id, ei_src_id, ei_trg_id, duration))


def TNEANet_test_io():
    # WRITE GRAPH TO FILE
    tneanet_ins = TNEANet_graph_creation_test()
    fd_out = snap.TFOut('test.graph')
    tneanet_ins.Save(fd_out)
    fd_out.Flush()

    # READ GRAPH FROM FILE
    fd_in = snap.TFIn('test.graph')
    tneanet_ins = snap.TNEANet.Load(fd_in)
    TNEANet_test_in_memory(tneanet_ins)


def TNEANet_to_TTables_test():
    tneanet_ins = TNEANet_graph_creation_test()

    context = snap.TTableContext()
    node_ttable = snap.TTable.GetNodeTable(tneanet_ins, context)
    edge_ttable = snap.TTable.GetEdgeTable(tneanet_ins, context)

    print()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    # tneanet_ins = TNEANet_graph_creation_test()
    # TNEANet_test_in_memory(tneanet_ins)
    # TNEANet_test_io()
    TNEANet_to_TTables_test()

    print('Done.')