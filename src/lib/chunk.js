const chunk = (arr, size) => {
  const arrs = [];
  const clone = [...arr];

  while (clone.length > 0) {
    const out = clone.splice(0, size);
    arrs.push(out);
  }

  return arrs;
};

module.exports = {
  chunk,
};
