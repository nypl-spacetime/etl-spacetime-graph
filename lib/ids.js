function expandId (datasetId, id) {
  if (String(id).includes('/')) {
    return id
  } else {
    return `${datasetId}/${id}`
  }
}

module.exports = {
  expand: expandId
}
