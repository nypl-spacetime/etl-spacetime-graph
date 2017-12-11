const fs = require('fs')
const path = require('path')
const R = require('ramda')
const H = require('highland')

const IGNORED_DIRS = [
  'node_modules',
  '.git'
]

function isDirectory (dataset) {
  if (dataset.id === '.' || IGNORED_DIRS.indexOf(dataset.id) > -1) {
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

function readAllFiles (baseDir, config, type) {
  if (!type) {
    return []
  }

  const exclude = config.exclude || []

  return fs.readdirSync(baseDir)
    .map((file) => ({
      id: file,
      dir: baseDir
    }))
    .filter(isDirectory)
    .filter(R.curry(containsFile)(type))
    .filter((dataset) => !exclude.includes(dataset.id))
    .map((dataset) => Object.assign(dataset, {
      type,
      filename: getFilename(type, dataset)
    }))
    .map(readFile)
}

module.exports = readAllFiles
