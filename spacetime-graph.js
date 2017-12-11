const path = require('path')
const R = require('ramda')
const H = require('highland')

const Graph = require('./lib/graph')
const Concept = require('./lib/concept')

const readAllFiles = require('./lib/read-files')
const ids = require('./lib/ids')

function addToGraph (graph, obj) {
  if (obj.type === 'object') {
    const id = ids.expand(obj.datasetId, obj.data.id)
    graph.addNode(id, obj)
  } else if (obj.type === 'relation') {
    const from = ids.expand(obj.datasetId, obj.data.from)
    const to = ids.expand(obj.datasetId, obj.data.to)
    graph.addEdge(from, to, obj.data.type)
  }

  return obj
}

function aggregate (config, dirs, tools, callback) {
  const step = 'transform'
  const baseDir = path.join(dirs.current, '..', '..', step)

  const allObjects = readAllFiles(baseDir, config, 'objects')
  const allRelations = readAllFiles(baseDir, config, 'relations')

  const graph = Graph()
  const concept = Concept(graph)
  let graphCount = 0

  H([
    allObjects,
    allRelations
  ])
    .flatten()
    .compact()
    .map(R.curry(addToGraph)(graph))
    .errors((err) => {
      // TODO: log errors to file!
      console.error(err.message)
    })
    .map((obj) => {
      if (graphCount > 0 && graphCount % 100000 === 0) {
        console.log(`      Added ${graphCount} items to graph`)
      }

      graphCount += 1

      return obj
    })
    .done(() => {
      console.log('    Done!')
      let conceptCount = 0

      H(graph.components('st:sameAs'))
        .map(concept.make)
        .compact()
        .map((concept) => ({
          type: 'object',
          obj: concept
        }))
        .map((concept) => {
          if (conceptCount > 0 && conceptCount % 5000 === 0) {
            console.log(`      Written ${conceptCount} concepts to file`)
          }
          conceptCount += 1

          return concept
        })
        .map(H.curry(tools.writer.writeObject))
        .nfcall([])
        .series()
        .errors(callback)
        .done(callback)
    })
}

// ==================================== API ====================================

module.exports.steps = [
  aggregate
]
