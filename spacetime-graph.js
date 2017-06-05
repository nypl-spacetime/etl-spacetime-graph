const fs = require('fs')
const path = require('path')
const R = require('ramda')
const H = require('highland')
const graphlib = require('graphlib')
const crypto = require('crypto')

const graph = new graphlib.Graph({
  directed: true,
  compound: true,
  multigraph: true
})

const kvStore = require('./kv-store')

const ignoredDirs = [
  'node_modules',
  '.git'
]

function isDirectory (dataset) {
  if (dataset.id === '.' || ignoredDirs.indexOf(dataset.id) > -1) {
    return false
  }

  var stat = fs.statSync(path.join(dataset.dir, dataset.id))
  return stat.isDirectory()
}

function getFilename (type, dataset) {
  return path.join(dataset.dir, dataset.id, `${dataset.id}.${type}.ndjson`)
}

function containsFile (type, dataset) {
  return fs.existsSync(getFilename(type, dataset))
}

function expandId (datasetId, id) {
  if (String(id).includes('/')) {
    return id
  } else {
    return `${datasetId}/${id}`
  }
}

function allFiles (baseDir, type) {
  if (!type) {
    return []
  }

  return fs.readdirSync(baseDir)
    .map((file) => ({
      id: file,
      dir: baseDir
    }))
    .filter(isDirectory)
    .filter(R.curry(containsFile)(type))
    .map((dataset) => Object.assign(dataset, {
      type,
      filename: getFilename(type, dataset)
    }))
}

function readFile (dataset) {
  return H(fs.createReadStream(dataset.filename))
    .split()
    .compact()
    .map(JSON.parse)
    .map((obj) => ({
      type: dataset.type === 'objects' ? 'object' : 'relation',
      datasetId: dataset.id,
      data: obj
    }))
}

function store (kvStore, obj) {
  if (obj.type === 'object') {
    const id = expandId(obj.datasetId, obj.data.id)
    kvStore.set(id, obj)
  } else if (obj.type === 'relation') {
    const from = expandId(obj.datasetId, obj.data.from)
    const to = expandId(obj.datasetId, obj.data.to)

    let object = kvStore.get(from)

    if (!object) {
      throw new Error(`Object '${from}' not found in key-value store`)
    }

    if (!object.relations) {
      object.relations = []
    }

    object.relations.push({
      to,
      type: obj.data.type
    })
  }

  return obj
}

function addToGraph (graph, obj) {
  // TODO: gooooood URIs - use URI module
  if (obj.type === 'object') {
    const id = expandId(obj.datasetId, obj.data.id)
    graph.setNode(id)
  } else if (obj.type === 'relation') {
    if (obj.data.type === 'st:sameAs') {
      const from = expandId(obj.datasetId, obj.data.from)
      const to = expandId(obj.datasetId, obj.data.to)
      graph.setEdge(from, to)
    }
  }

  return obj
}

function makeConceptGeometry (objects) {
  const geometries = objects
    .map(R.prop('data'))
    .filter(R.has('geometry'))
    .map(R.prop('geometry'))

  if (geometries.length === 1) {
    return geometries[0]
  } else if (geometries.length > 1) {
    return {
      type: 'GeometryCollection',
      geometries: geometries
    }
  }
}

// "hairs": [
//   {
//     "type": "hg:Municipality",
//     "uri": "http://sws.geonames.org/2758063/",
//     "name": "Gemeente Bussum",
//     "@id": "http://sws.geonames.org/2758063/"
//   }
// ],

function makeConceptObjects (objects) {
  const geometry = makeConceptGeometry(objects)
  const isGeometryCollection = geometry && geometry.type === 'GeometryCollection'

  let geometryIndex = 0
  const getGeometryIndex = (geometry) => {
    if (isGeometryCollection) {
      return geometry ? geometryIndex++ : -1
    }
  }

  return objects
    .map((object) => Object.assign(object.data, {
      id: expandId(object.datasetId, object.data.id),
      geometryIndex: getGeometryIndex(object.data.geometry),
      // hairs: makeConceptHairs(objects),
      relations: object.relations
    }))
    .map(R.omit('geometry'))
}

function makeConceptId (objects) {
  const ids = objects
    .map((object) => expandId(object.datasetId, object.data.id))
    .join('-')

  return crypto.createHash('md5').update(ids).digest('hex')
}

function makeConceptType (objects) {
  const firstType = objects[0].data.type
  const allSameType = objects
    .map(R.path(['data', 'type']))
    .reduce((acc, val) => acc === val, firstType)

  if (!allSameType) {
    // TODO: throw error, log error
  }

  return firstType
}

function makeConceptName (objects) {
  const names = objects
    .map(R.path(['data', 'name']))
    .filter(R.identity)
    .sort((a, b) => b.length - a.length)

  if (names.length) {
    return names[0]
  }
}

function makeConceptValidSince (objects) {

}

function makeConceptValidUntil (objects) {

}

function makeConcept (kvStore, component) {
  const objects = component.map(kvStore.get)
    .filter(R.identity)

  return {
    id: makeConceptId(objects),
    type: makeConceptType(objects),
    name: makeConceptName(objects),
    validSince: makeConceptValidSince(objects),
    validUntil: makeConceptValidUntil(objects),
    data: {
      objects: makeConceptObjects(objects)
    },
    geometry: makeConceptGeometry(objects)
  }
}

function dfs (graph, vertex, visited) {
  const doDfs = (vertex) => {
    if (visited[vertex]) {
      return
    }

    visited[vertex] = true

    return R.flatten([
      graph.predecessors(vertex).map(doDfs),
      vertex,
      graph.successors(vertex).map(doDfs)
    ]).filter(R.identity)
  }

  return doDfs(vertex)
}

function aggregate (config, dirs, tools, callback) {
  const step = 'transform'
  const baseDir = path.join(dirs.current, '..', '..', step)

  const allObjects = allFiles(baseDir, 'objects')
  const allRelations = allFiles(baseDir, 'relations')

  let i = 0

  H([
    allObjects,
    allRelations
  ])
    .flatten()
    .map(readFile)
    .mergeWithLimit(1)
    .map(R.curry(store)(kvStore))
    .errors((err) => {
      // TODO: log errors to log file
    })
    .map(R.curry(addToGraph)(graph))
    .each((obj) => {
      if (i > 0 && i % 100000 === 0) {
        console.log(`      Added ${i} items to graph`)
      }

      i += 1

      return obj
    })
    .errors(console.error)
    .done(() => {
      console.log(`    Done!`)
      let c = 0

      const visited = {}
      H(graph.nodes())
        .map(R.curry(dfs)(graph, R.__, visited))
        .compact()
        .map(R.curry(makeConcept)(kvStore))
        .map((concept) => ({
          type: 'object',
          obj: concept
        }))
        .map((concept) => {
          if (c > 0 && c % 5000 === 0) {
            console.log(`      Written ${c} concepts to file`)
          }
          c += 1

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
