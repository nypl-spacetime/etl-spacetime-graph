const R = require('ramda')

module.exports = function () {
  const graph = new Map()

  function addNode (id, data) {
    if (graph.has(id)) {
      throw new Error(`Node with ID ${id} already exists`)
    }

    graph.set(id, {
      data,
      incoming: {},
      outgoing: {}
    })
  }

  function getNode (id) {
    return graph.get(id).data
  }

  function getOutgoing (id, type) {
    const edges = graph.get(id).outgoing
    return getNodesFromEdges(edges, type)
  }

  function getIncoming (id, type) {
    const edges = graph.get(id).incoming
    return getNodesFromEdges(edges, type)
  }

  function getNodesFromEdges (edges, type) {
    if (!edges) {
      return
    }

    if (type) {
      const ids = edges[type]

      if (ids) {
        return Array.from(ids)
      }
    } else {
      return R.fromPairs(Object.entries(edges).map((pair) => [pair[0], [...pair[1]]]))
    }
  }

  function addToEdges (edges, id, type) {
    if (!type) {
      throw new Error('Edge type not set')
    }

    let typeSet = edges[type]
    if (!typeSet) {
      typeSet = new Set()
      edges[type] = typeSet
    }

    typeSet.add(id)
  }

  function addEdge (idFrom, idTo, type) {
    const nodeFrom = graph.get(idFrom)
    const nodeTo = graph.get(idTo)

    if (!nodeFrom) {
      throw new Error(`Node with ID ${idFrom} does not exist`)
    }

    if (!nodeTo) {
      throw new Error(`Node with ID ${idTo} does not exist`)
    }

    addToEdges(nodeFrom.outgoing, idTo, type)
    addToEdges(nodeTo.incoming, idFrom, type)
  }

  function nodes () {
    return graph.keys()
  }

  function* components (type) {
    const visited = new Set()

    const doDfs = (id) => {
      if (visited.has(id)) {
        return []
      }

      visited.add(id)

      const incoming = getIncoming(id, type) || []
      const outgoing = getOutgoing(id, type) || []

      return [
        ...R.flatten(incoming.map(doDfs)),
        id,
        ...R.flatten(outgoing.map(doDfs))
      ]
    }

    const iter = graph.keys()
    let node = iter.next()

    while (!node.done) {
      const component = doDfs(node.value)
      if (component && component.length) {
        yield component
      }

      node = iter.next()
    }
  }

  return {
    addNode,
    addEdge,
    getNode,
    getOutgoing,
    getIncoming,
    nodes,
    components
  }
}

