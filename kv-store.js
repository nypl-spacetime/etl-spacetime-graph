let kvStore = null

function set (key, value) {
  if (!kvStore) {
    kvStore = {}
  }

  kvStore[key] = value
}

function get (key) {
  if (kvStore) {
    return kvStore[key]
  }
}

function clear () {
  kvStore = null
}

module.exports = {
  get,
  set,
  clear
}
