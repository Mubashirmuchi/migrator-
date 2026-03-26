function transform(row) {
  // Customize per client
  return {
    ...row,
    // example:
    // status: row.status?.toUpperCase()
  };
}

module.exports = { transform };
