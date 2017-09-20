const R = require('ramda')
const base62 = require('base-62.js')
const crypto = require('crypto')
const fuzzyDates = require('fuzzy-dates')

const ids = require('./ids')

module.exports = function (graph) {
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

  function addRelationNodeData (id) {
    const object = graph.getNode(id).data
    return {
      id,
      type: object.type,
      name: object.name
    }
  }

  function addRelationsNodeData (relations) {
    return R.fromPairs(R.toPairs(relations)
      .filter((pair) => pair[0] !== 'st:sameAs')
      .map((pair) => ([
        pair[0],
        pair[1].map(addRelationNodeData)
      ]))
    )
  }

  function makeObjectRelations (object) {
    const objectId = ids.expand(object.datasetId, object.data.id)

    const incoming = addRelationsNodeData(graph.getIncoming(objectId))
    const outgoing = addRelationsNodeData(graph.getOutgoing(objectId))

    return {
      incoming,
      outgoing
    }
  }

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
        id: ids.expand(object.datasetId, object.data.id),
        dataset: object.datasetId,
        geometryIndex: getGeometryIndex(object.data.geometry),
        relations: makeObjectRelations(object)
      }))
      .map(R.omit('geometry'))
  }

  function makeConceptId (objects) {
    const objectIds = objects
      .map((object) => ids.expand(object.datasetId, object.data.id))
      .join('-')

    const hash = crypto.createHash('md5').update(objectIds).digest('hex')
    return base62.encodeHex(hash)
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
    // TODO: don't choose longest one, but have a list of preferred datasets
    const names = objects
      .map(R.path(['data', 'name']))
      .filter(R.identity)
      .sort((a, b) => b.length - a.length)

    if (names.length) {
      return names[0]
    }
  }

  function makeConceptValidSince (objects) {
    const validSince = R.path(['data', 'validSince'])
    const toDate = (d) => new Date(fuzzyDates.convert(d.date)[0])

    const dates = objects
      .map(validSince)
      .map((date, index) => ({
        date, index
      }))
      .filter(R.prop('date'))
      .sort((a, b) => toDate(b) - toDate(a))

    if (dates.length) {
      return validSince(objects[dates[0].index])
    }
  }

  function makeConceptValidUntil (objects) {
    const validUntil = R.path(['data', 'validUntil'])
    const toDate = (d) => new Date(fuzzyDates.convert(d.date)[1])

    const dates = objects
      .map(validUntil)
      .map((date, index) => ({
        date, index
      }))
      .filter(R.prop('date'))
      .sort((a, b) => toDate(a) - toDate(b))

    if (dates.length) {
      return validUntil(objects[dates[0].index])
    }
  }

  function makeConcept (component) {
    const objects = component.map(graph.getNode)
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

  return {
    make: makeConcept
  }
}
