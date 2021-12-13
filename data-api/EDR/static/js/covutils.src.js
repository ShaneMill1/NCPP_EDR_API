(function (exports) {
	'use strict';

	function minMax(arr) {
	  var len = arr.length;
	  var min = Infinity;
	  var max = -Infinity;
	  while (len--) {
	    var el = arr[len];
	    if (el == null) {
	      // do nothing
	    } else if (el < min) {
	      min = el;
	    } else if (el > max) {
	      max = el;
	    }
	  }
	  if (min === Infinity) {
	    min = max;
	  } else if (max === -Infinity) {
	    max = min;
	  }
	  if (min === Infinity || min === -Infinity) {
	    // all values were null
	    min = null;
	    max = null;
	  }
	  return [min, max];
	}

	/**
	 * Return the indices of the two neighbors in the sorted array closest to the given number.
	 *
	 * @example
	 * var a = [2,5,8,12,13]
	 * var i = CovUtils.indicesOfNearest(a, 6)
	 * // i == [1,2]
	 * var j = CovUtils.indicesOfNearest(a, 5)
	 * // j == [1,1]
	 * var k = CovUtils.indicesOfNearest(a, 50)
	 * // k == [4,4]
	 *
	 * @param {Array<number>} a The array to search through. Must be sorted, ascending or descending.
	 * @param {number} x The target number.
	 * @return {[lo,hi]} The indices of the two closest values, may be equal.
	 *   If `x` exists in the array, both neighbors point to `x`.
	 *   If `x` is lower (greater if descending) than the first value, both neighbors point to 0.
	 *   If `x` is greater (lower if descending) than the last value, both neighbors point to the last index.
	 */
	function indicesOfNearest(a, x) {
	  if (a.length === 0) {
	    throw new Error('Array must have at least one element');
	  }
	  var lo = -1;
	  var hi = a.length;
	  var ascending = a.length === 1 || a[0] < a[1];
	  // we have two separate code paths to help the runtime optimize the loop
	  if (ascending) {
	    while (hi - lo > 1) {
	      var mid = Math.round((lo + hi) / 2);
	      if (a[mid] <= x) {
	        lo = mid;
	      } else {
	        hi = mid;
	      }
	    }
	  } else {
	    while (hi - lo > 1) {
	      var _mid = Math.round((lo + hi) / 2);
	      if (a[_mid] >= x) {
	        // here's the difference
	        lo = _mid;
	      } else {
	        hi = _mid;
	      }
	    }
	  }
	  if (a[lo] === x) hi = lo;
	  if (lo === -1) lo = hi;
	  if (hi === a.length) hi = lo;
	  return [lo, hi];
	}

	/**
	 * Return the index of the value closest to the given number in a sorted array.
	 *
	 * @example
	 * var a = [2,5,8,12,13]
	 * var i = CovUtils.indexOfNearest(a, 6)
	 * // i == 1
	 * var j = CovUtils.indexOfNearest(a, 7)
	 * // j == 2
	 * var k = CovUtils.indexOfNearest(a, 50)
	 * // k == 4
	 *
	 * @param {Array<number>} a The array to search through. Must be sorted, ascending or descending.
	 * @param {number} x The target number.
	 * @return {number} The array index whose value is closest to `x`.
	 *   If `x` happens to be exactly between two values, then the lower index is returned.
	 */
	function indexOfNearest(a, x) {
	  var i = indicesOfNearest(a, x);
	  var lo = i[0];
	  var hi = i[1];
	  if (Math.abs(x - a[lo]) <= Math.abs(x - a[hi])) {
	    return lo;
	  } else {
	    return hi;
	  }
	}

	var DOMAIN = 'Domain';
	var COVERAGE = 'Coverage';
	var COVERAGECOLLECTION = COVERAGE + 'Collection';

	var COVJSON_NS = 'http://covjson.org/def/core#';

	var COVJSON_DATATYPE_TUPLE = COVJSON_NS + 'tuple';
	var COVJSON_DATATYPE_POLYGON = COVJSON_NS + 'polygon';

	var DEFAULT_LANGUAGE = 'en';

	/**
	 * @example
	 * var labels = {'en': 'Temperature', 'de': 'Temperatur'}
	 * var tag = CovUtils.getLanguageTag(labels, 'en-GB')
	 * // tag == 'en'
	 *
	 * @param {object} map An object that maps language tags to strings.
	 * @param {string} [preferredLanguage='en'] The preferred language as a language tag, e.g. 'de'.
	 * @return {string} The best matched language tag of the input map.
	 *   If no match was found then this is an arbitrary tag of the map.
	 */
	function getLanguageTag(map) {
	  var preferredLanguage = arguments.length <= 1 || arguments[1] === undefined ? DEFAULT_LANGUAGE : arguments[1];

	  if (preferredLanguage in map) {
	    return preferredLanguage;
	  }

	  // cut off any subtags following the language subtag and try to find a match
	  var prefTag = preferredLanguage.split('-')[0];
	  var matches = Object.keys(map).filter(function (tag) {
	    return prefTag === tag.split('-')[0];
	  });
	  if (matches.length) {
	    return matches[0];
	  }

	  // no luck, return a random tag
	  return Object.keys(map)[0];
	}

	/**
	 * @example
	 * var labels = {'en': 'Temperature', 'de': 'Temperatur'}
	 * var label = CovUtils.getLanguageString(labels, 'en-GB')
	 * // label == 'Temperature'
	 *
	 * @param {object} map An object that maps language tags to strings.
	 * @param {string} [preferredLanguage='en'] The preferred language as a language tag, e.g. 'de'.
	 * @return {string} The string within the input map whose language tag best matched.
	 *   If no match was found then this is an arbitrary string of the map.
	 */
	function getLanguageString(map) {
	  var preferredLanguage = arguments.length <= 1 || arguments[1] === undefined ? DEFAULT_LANGUAGE : arguments[1];

	  var tag = getLanguageTag(map, preferredLanguage);
	  return map[tag];
	}

	/**
	 * Converts a unit object to a human-readable symbol or label, where symbols are preferred.
	 *
	 * @example
	 * var unit = {
	 *   symbol: '°C'
	 * }
	 * var str = CovUtils.stringifyUnit(unit) // str == '°C'
	 *
	 * @example
	 * var unit = {
	 *   symbol: {
	 *     value: 'Cel',
	 *     type: 'http://www.opengis.net/def/uom/UCUM/'
	 *   },
	 *   label: {
	 *     en: 'Degree Celsius'
	 *   }
	 * }
	 * var str = CovUtils.stringifyUnit(unit) // str == '°C'
	 *
	 * @example
	 * var unit = {
	 *   label: {
	 *     en: 'Degree Celsius',
	 *     de: 'Grad Celsius'
	 *   }
	 * }
	 * var str = CovUtils.stringifyUnit(unit, 'en') // str == 'Degree Celsius'
	 */
	function stringifyUnit(unit, language) {
	  if (!unit) {
	    return '';
	  }
	  if (unit.symbol) {
	    var symbol = unit.symbol.value || unit.symbol;
	    var scheme = unit.symbol.type;
	    if (scheme === 'http://www.opengis.net/def/uom/UCUM/') {
	      if (symbol === 'Cel') {
	        symbol = '°C';
	      } else if (symbol === '1') {
	        symbol = '';
	      }
	    }
	    return symbol;
	  } else {
	    return getLanguageString(unit.label, language);
	  }
	}

	var _typeof = typeof Symbol === "function" && typeof Symbol.iterator === "symbol" ? function (obj) {
	  return typeof obj;
	} : function (obj) {
	  return obj && typeof Symbol === "function" && obj.constructor === Symbol ? "symbol" : typeof obj;
	};

	var classCallCheck = function (instance, Constructor) {
	  if (!(instance instanceof Constructor)) {
	    throw new TypeError("Cannot call a class as a function");
	  }
	};

	var createClass = function () {
	  function defineProperties(target, props) {
	    for (var i = 0; i < props.length; i++) {
	      var descriptor = props[i];
	      descriptor.enumerable = descriptor.enumerable || false;
	      descriptor.configurable = true;
	      if ("value" in descriptor) descriptor.writable = true;
	      Object.defineProperty(target, descriptor.key, descriptor);
	    }
	  }

	  return function (Constructor, protoProps, staticProps) {
	    if (protoProps) defineProperties(Constructor.prototype, protoProps);
	    if (staticProps) defineProperties(Constructor, staticProps);
	    return Constructor;
	  };
	}();

	var defineProperty = function (obj, key, value) {
	  if (key in obj) {
	    Object.defineProperty(obj, key, {
	      value: value,
	      enumerable: true,
	      configurable: true,
	      writable: true
	    });
	  } else {
	    obj[key] = value;
	  }

	  return obj;
	};

	var slicedToArray = function () {
	  function sliceIterator(arr, i) {
	    var _arr = [];
	    var _n = true;
	    var _d = false;
	    var _e = undefined;

	    try {
	      for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) {
	        _arr.push(_s.value);

	        if (i && _arr.length === i) break;
	      }
	    } catch (err) {
	      _d = true;
	      _e = err;
	    } finally {
	      try {
	        if (!_n && _i["return"]) _i["return"]();
	      } finally {
	        if (_d) throw _e;
	      }
	    }

	    return _arr;
	  }

	  return function (arr, i) {
	    if (Array.isArray(arr)) {
	      return arr;
	    } else if (Symbol.iterator in Object(arr)) {
	      return sliceIterator(arr, i);
	    } else {
	      throw new TypeError("Invalid attempt to destructure non-iterable instance");
	    }
	  };
	}();

	var toConsumableArray = function (arr) {
	  if (Array.isArray(arr)) {
	    for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) arr2[i] = arr[i];

	    return arr2;
	  } else {
	    return Array.from(arr);
	  }
	};

	/**
	 * @external {Range} https://github.com/Reading-eScience-Centre/coverage-jsapi/blob/master/Range.md
	 */

	/**
	 * Return the minimum/maximum across all range values, ignoring null's.
	 *
	 * @param {Range<number>} range The numeric coverage data range.
	 * @return {[min,max]} The minimum and maximum values of the range,
	 *   or [undefined, undefined] if the range contains only `null` values.
	 */
	function minMaxOfRange(range) {
	  var min = Infinity;
	  var max = -Infinity;
	  var fn = function fn(val) {
	    if (val === null) return;
	    if (val < min) min = val;
	    if (val > max) max = val;
	  };
	  iterateRange(range, fn);
	  return min === Infinity ? [undefined, undefined] : [min, max];
	}

	/**
	 * Apply a reduce function over the range values.
	 *
	 * @param {Range} range The coverage data range.
	 * @param {function} callback Function to execute on each value in the array with arguments `(previousValue, currentValue)`.
	 * @param start Value to use as the first argument to the first call of the `callback`.
	 * @return The reduced value.
	 */
	function reduceRange(range, callback, start) {
	  var v1 = start;
	  var iterFn = function iterFn(v2) {
	    v1 = callback(v1, v2);
	  };
	  iterateRange(range, iterFn);
	  return v1;
	}

	/**
	 * Iterate over all range values and run a function for each value.
	 * No particular iteration order must be assumed.
	 */
	function iterateRange(range, fn) {
	  // We use a precompiled function here for efficiency.
	  // See below for a slower recursive version.

	  // Benchmarks compared to recursive version:
	  // Chrome 46: around 1.03x faster
	  // Firefox 42: around 2x faster (and around 6x faster than Chrome 46!)

	  // nest loops from smallest to biggest
	  var shape = [].concat(toConsumableArray(range.shape));
	  shape.sort(function (_ref, _ref2) {
	    var _ref4 = slicedToArray(_ref, 2);

	    var size1 = _ref4[1];

	    var _ref3 = slicedToArray(_ref2, 2);

	    var size2 = _ref3[1];
	    return size1 - size2;
	  });

	  var begin = 'var obj = {}';
	  var end = '';
	  var _iteratorNormalCompletion = true;
	  var _didIteratorError = false;
	  var _iteratorError = undefined;

	  try {
	    for (var _iterator = shape[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
	      var _step$value = slicedToArray(_step.value, 2);

	      var axis = _step$value[0];
	      var size = _step$value[1];

	      begin += '\n      for (var i' + axis + '=0; i' + axis + ' < ' + size + '; ++i' + axis + ') {\n        obj[\'' + axis + '\'] = i' + axis + '\n    ';
	      end += '}';
	    }
	  } catch (err) {
	    _didIteratorError = true;
	    _iteratorError = err;
	  } finally {
	    try {
	      if (!_iteratorNormalCompletion && _iterator.return) {
	        _iterator.return();
	      }
	    } finally {
	      if (_didIteratorError) {
	        throw _iteratorError;
	      }
	    }
	  }

	  begin += '\n    fn(get(obj))\n  ';

	  var iterateLoop = new Function('return function iterRange (get, fn) { ' + begin + ' ' + end + ' }')(); // eslint-disable-line
	  iterateLoop(range.get, fn);
	}

	/**
	 * Returns the category of the given parameter corresponding to the encoded integer value.
	 *
	 * @param {Parameter} parameter
	 * @param {number} val
	 * @return {Category}
	 */
	function getCategory(parameter, val) {
	  var _iteratorNormalCompletion = true;
	  var _didIteratorError = false;
	  var _iteratorError = undefined;

	  try {
	    var _loop = function _loop() {
	      var _step$value = slicedToArray(_step.value, 2);

	      var catId = _step$value[0];
	      var vals = _step$value[1];

	      if (vals.indexOf(val) !== -1) {
	        var cat = parameter.observedProperty.categories.filter(function (c) {
	          return c.id === catId;
	        })[0];
	        return {
	          v: cat
	        };
	      }
	    };

	    for (var _iterator = parameter.categoryEncoding[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
	      var _ret = _loop();

	      if ((typeof _ret === "undefined" ? "undefined" : _typeof(_ret)) === "object") return _ret.v;
	    }
	  } catch (err) {
	    _didIteratorError = true;
	    _iteratorError = err;
	  } finally {
	    try {
	      if (!_iteratorNormalCompletion && _iterator.return) {
	        _iterator.return();
	      }
	    } finally {
	      if (_didIteratorError) {
	        throw _iteratorError;
	      }
	    }
	  }
	}

	function isCoverage(obj) {
	  return obj.type === COVERAGE;
	}

	function checkCoverage(obj) {
	  if (!isCoverage(obj)) {
	    throw new Error('must be a Coverage');
	  }
	}

	function isDomain(obj) {
	  return obj.type === DOMAIN;
	}

	function checkDomain(obj) {
	  if (!isDomain(obj)) {
	    throw new Error('must be a Domain');
	  }
	}

	function createCommonjsModule(fn, module) {
		return module = { exports: {} }, fn(module, module.exports), module.exports;
	}

	var imlfn = createCommonjsModule(function (module) {
	module.exports = function(ml, e0, e1, e2, e3) {
	  var phi;
	  var dphi;

	  phi = ml / e0;
	  for (var i = 0; i < 15; i++) {
	    dphi = (ml - (e0 * phi - e1 * Math.sin(2 * phi) + e2 * Math.sin(4 * phi) - e3 * Math.sin(6 * phi))) / (e0 - 2 * e1 * Math.cos(2 * phi) + 4 * e2 * Math.cos(4 * phi) - 6 * e3 * Math.cos(6 * phi));
	    phi += dphi;
	    if (Math.abs(dphi) <= 0.0000000001) {
	      return phi;
	    }
	  }

	  //..reportError("IMLFN-CONV:Latitude failed to converge after 15 iterations");
	  return NaN;
	};
	});

	var require$$0$3 = (imlfn && typeof imlfn === 'object' && 'default' in imlfn ? imlfn['default'] : imlfn);

	var asinz = createCommonjsModule(function (module) {
	module.exports = function(x) {
	  if (Math.abs(x) > 1) {
	    x = (x > 1) ? 1 : -1;
	  }
	  return Math.asin(x);
	};
	});

	var require$$0$4 = (asinz && typeof asinz === 'object' && 'default' in asinz ? asinz['default'] : asinz);

	var gN = createCommonjsModule(function (module) {
	module.exports = function(a, e, sinphi) {
	  var temp = e * sinphi;
	  return a / Math.sqrt(1 - temp * temp);
	};
	});

	var require$$3 = (gN && typeof gN === 'object' && 'default' in gN ? gN['default'] : gN);

	var e3fn = createCommonjsModule(function (module) {
	module.exports = function(x) {
	  return (x * x * x * (35 / 3072));
	};
	});

	var require$$4 = (e3fn && typeof e3fn === 'object' && 'default' in e3fn ? e3fn['default'] : e3fn);

	var e2fn = createCommonjsModule(function (module) {
	module.exports = function(x) {
	  return (0.05859375 * x * x * (1 + 0.75 * x));
	};
	});

	var require$$5 = (e2fn && typeof e2fn === 'object' && 'default' in e2fn ? e2fn['default'] : e2fn);

	var e1fn = createCommonjsModule(function (module) {
	module.exports = function(x) {
	  return (0.375 * x * (1 + 0.25 * x * (1 + 0.46875 * x)));
	};
	});

	var require$$6 = (e1fn && typeof e1fn === 'object' && 'default' in e1fn ? e1fn['default'] : e1fn);

	var e0fn = createCommonjsModule(function (module) {
	module.exports = function(x) {
	  return (1 - 0.25 * x * (1 + x / 16 * (3 + 1.25 * x)));
	};
	});

	var require$$7 = (e0fn && typeof e0fn === 'object' && 'default' in e0fn ? e0fn['default'] : e0fn);

	var mlfn = createCommonjsModule(function (module) {
	module.exports = function(e0, e1, e2, e3, phi) {
	  return (e0 * phi - e1 * Math.sin(2 * phi) + e2 * Math.sin(4 * phi) - e3 * Math.sin(6 * phi));
	};
	});

	var require$$3$1 = (mlfn && typeof mlfn === 'object' && 'default' in mlfn ? mlfn['default'] : mlfn);

	var sign = createCommonjsModule(function (module) {
	module.exports = function(x) {
	  return x<0 ? -1 : 1;
	};
	});

	var require$$1 = (sign && typeof sign === 'object' && 'default' in sign ? sign['default'] : sign);

	var adjust_lon = createCommonjsModule(function (module) {
	var TWO_PI = Math.PI * 2;
	// SPI is slightly greater than Math.PI, so values that exceed the -180..180
	// degree range by a tiny amount don't get wrapped. This prevents points that
	// have drifted from their original location along the 180th meridian (due to
	// floating point error) from changing their sign.
	var SPI = 3.14159265359;
	var sign = require$$1;

	module.exports = function(x) {
	  return (Math.abs(x) <= SPI) ? x : (x - (sign(x) * TWO_PI));
	};
	});

	var require$$2 = (adjust_lon && typeof adjust_lon === 'object' && 'default' in adjust_lon ? adjust_lon['default'] : adjust_lon);

	var aeqd = createCommonjsModule(function (module, exports) {
	var adjust_lon = require$$2;
	var HALF_PI = Math.PI/2;
	var EPSLN = 1.0e-10;
	var mlfn = require$$3$1;
	var e0fn = require$$7;
	var e1fn = require$$6;
	var e2fn = require$$5;
	var e3fn = require$$4;
	var gN = require$$3;
	var asinz = require$$0$4;
	var imlfn = require$$0$3;
	exports.init = function() {
	  this.sin_p12 = Math.sin(this.lat0);
	  this.cos_p12 = Math.cos(this.lat0);
	};

	exports.forward = function(p) {
	  var lon = p.x;
	  var lat = p.y;
	  var sinphi = Math.sin(p.y);
	  var cosphi = Math.cos(p.y);
	  var dlon = adjust_lon(lon - this.long0);
	  var e0, e1, e2, e3, Mlp, Ml, tanphi, Nl1, Nl, psi, Az, G, H, GH, Hs, c, kp, cos_c, s, s2, s3, s4, s5;
	  if (this.sphere) {
	    if (Math.abs(this.sin_p12 - 1) <= EPSLN) {
	      //North Pole case
	      p.x = this.x0 + this.a * (HALF_PI - lat) * Math.sin(dlon);
	      p.y = this.y0 - this.a * (HALF_PI - lat) * Math.cos(dlon);
	      return p;
	    }
	    else if (Math.abs(this.sin_p12 + 1) <= EPSLN) {
	      //South Pole case
	      p.x = this.x0 + this.a * (HALF_PI + lat) * Math.sin(dlon);
	      p.y = this.y0 + this.a * (HALF_PI + lat) * Math.cos(dlon);
	      return p;
	    }
	    else {
	      //default case
	      cos_c = this.sin_p12 * sinphi + this.cos_p12 * cosphi * Math.cos(dlon);
	      c = Math.acos(cos_c);
	      kp = c / Math.sin(c);
	      p.x = this.x0 + this.a * kp * cosphi * Math.sin(dlon);
	      p.y = this.y0 + this.a * kp * (this.cos_p12 * sinphi - this.sin_p12 * cosphi * Math.cos(dlon));
	      return p;
	    }
	  }
	  else {
	    e0 = e0fn(this.es);
	    e1 = e1fn(this.es);
	    e2 = e2fn(this.es);
	    e3 = e3fn(this.es);
	    if (Math.abs(this.sin_p12 - 1) <= EPSLN) {
	      //North Pole case
	      Mlp = this.a * mlfn(e0, e1, e2, e3, HALF_PI);
	      Ml = this.a * mlfn(e0, e1, e2, e3, lat);
	      p.x = this.x0 + (Mlp - Ml) * Math.sin(dlon);
	      p.y = this.y0 - (Mlp - Ml) * Math.cos(dlon);
	      return p;
	    }
	    else if (Math.abs(this.sin_p12 + 1) <= EPSLN) {
	      //South Pole case
	      Mlp = this.a * mlfn(e0, e1, e2, e3, HALF_PI);
	      Ml = this.a * mlfn(e0, e1, e2, e3, lat);
	      p.x = this.x0 + (Mlp + Ml) * Math.sin(dlon);
	      p.y = this.y0 + (Mlp + Ml) * Math.cos(dlon);
	      return p;
	    }
	    else {
	      //Default case
	      tanphi = sinphi / cosphi;
	      Nl1 = gN(this.a, this.e, this.sin_p12);
	      Nl = gN(this.a, this.e, sinphi);
	      psi = Math.atan((1 - this.es) * tanphi + this.es * Nl1 * this.sin_p12 / (Nl * cosphi));
	      Az = Math.atan2(Math.sin(dlon), this.cos_p12 * Math.tan(psi) - this.sin_p12 * Math.cos(dlon));
	      if (Az === 0) {
	        s = Math.asin(this.cos_p12 * Math.sin(psi) - this.sin_p12 * Math.cos(psi));
	      }
	      else if (Math.abs(Math.abs(Az) - Math.PI) <= EPSLN) {
	        s = -Math.asin(this.cos_p12 * Math.sin(psi) - this.sin_p12 * Math.cos(psi));
	      }
	      else {
	        s = Math.asin(Math.sin(dlon) * Math.cos(psi) / Math.sin(Az));
	      }
	      G = this.e * this.sin_p12 / Math.sqrt(1 - this.es);
	      H = this.e * this.cos_p12 * Math.cos(Az) / Math.sqrt(1 - this.es);
	      GH = G * H;
	      Hs = H * H;
	      s2 = s * s;
	      s3 = s2 * s;
	      s4 = s3 * s;
	      s5 = s4 * s;
	      c = Nl1 * s * (1 - s2 * Hs * (1 - Hs) / 6 + s3 / 8 * GH * (1 - 2 * Hs) + s4 / 120 * (Hs * (4 - 7 * Hs) - 3 * G * G * (1 - 7 * Hs)) - s5 / 48 * GH);
	      p.x = this.x0 + c * Math.sin(Az);
	      p.y = this.y0 + c * Math.cos(Az);
	      return p;
	    }
	  }


	};

	exports.inverse = function(p) {
	  p.x -= this.x0;
	  p.y -= this.y0;
	  var rh, z, sinz, cosz, lon, lat, con, e0, e1, e2, e3, Mlp, M, N1, psi, Az, cosAz, tmp, A, B, D, Ee, F;
	  if (this.sphere) {
	    rh = Math.sqrt(p.x * p.x + p.y * p.y);
	    if (rh > (2 * HALF_PI * this.a)) {
	      return;
	    }
	    z = rh / this.a;

	    sinz = Math.sin(z);
	    cosz = Math.cos(z);

	    lon = this.long0;
	    if (Math.abs(rh) <= EPSLN) {
	      lat = this.lat0;
	    }
	    else {
	      lat = asinz(cosz * this.sin_p12 + (p.y * sinz * this.cos_p12) / rh);
	      con = Math.abs(this.lat0) - HALF_PI;
	      if (Math.abs(con) <= EPSLN) {
	        if (this.lat0 >= 0) {
	          lon = adjust_lon(this.long0 + Math.atan2(p.x, - p.y));
	        }
	        else {
	          lon = adjust_lon(this.long0 - Math.atan2(-p.x, p.y));
	        }
	      }
	      else {
	        /*con = cosz - this.sin_p12 * Math.sin(lat);
	        if ((Math.abs(con) < EPSLN) && (Math.abs(p.x) < EPSLN)) {
	          //no-op, just keep the lon value as is
	        } else {
	          var temp = Math.atan2((p.x * sinz * this.cos_p12), (con * rh));
	          lon = adjust_lon(this.long0 + Math.atan2((p.x * sinz * this.cos_p12), (con * rh)));
	        }*/
	        lon = adjust_lon(this.long0 + Math.atan2(p.x * sinz, rh * this.cos_p12 * cosz - p.y * this.sin_p12 * sinz));
	      }
	    }

	    p.x = lon;
	    p.y = lat;
	    return p;
	  }
	  else {
	    e0 = e0fn(this.es);
	    e1 = e1fn(this.es);
	    e2 = e2fn(this.es);
	    e3 = e3fn(this.es);
	    if (Math.abs(this.sin_p12 - 1) <= EPSLN) {
	      //North pole case
	      Mlp = this.a * mlfn(e0, e1, e2, e3, HALF_PI);
	      rh = Math.sqrt(p.x * p.x + p.y * p.y);
	      M = Mlp - rh;
	      lat = imlfn(M / this.a, e0, e1, e2, e3);
	      lon = adjust_lon(this.long0 + Math.atan2(p.x, - 1 * p.y));
	      p.x = lon;
	      p.y = lat;
	      return p;
	    }
	    else if (Math.abs(this.sin_p12 + 1) <= EPSLN) {
	      //South pole case
	      Mlp = this.a * mlfn(e0, e1, e2, e3, HALF_PI);
	      rh = Math.sqrt(p.x * p.x + p.y * p.y);
	      M = rh - Mlp;

	      lat = imlfn(M / this.a, e0, e1, e2, e3);
	      lon = adjust_lon(this.long0 + Math.atan2(p.x, p.y));
	      p.x = lon;
	      p.y = lat;
	      return p;
	    }
	    else {
	      //default case
	      rh = Math.sqrt(p.x * p.x + p.y * p.y);
	      Az = Math.atan2(p.x, p.y);
	      N1 = gN(this.a, this.e, this.sin_p12);
	      cosAz = Math.cos(Az);
	      tmp = this.e * this.cos_p12 * cosAz;
	      A = -tmp * tmp / (1 - this.es);
	      B = 3 * this.es * (1 - A) * this.sin_p12 * this.cos_p12 * cosAz / (1 - this.es);
	      D = rh / N1;
	      Ee = D - A * (1 + A) * Math.pow(D, 3) / 6 - B * (1 + 3 * A) * Math.pow(D, 4) / 24;
	      F = 1 - A * Ee * Ee / 2 - D * Ee * Ee * Ee / 6;
	      psi = Math.asin(this.sin_p12 * Math.cos(Ee) + this.cos_p12 * Math.sin(Ee) * cosAz);
	      lon = adjust_lon(this.long0 + Math.asin(Math.sin(Az) * Math.sin(Ee) / Math.cos(psi)));
	      lat = Math.atan((1 - this.es * F * this.sin_p12 / Math.sin(psi)) * Math.tan(psi) / (1 - this.es));
	      p.x = lon;
	      p.y = lat;
	      return p;
	    }
	  }

	};
	exports.names = ["Azimuthal_Equidistant", "aeqd"];
	});

	var require$$0$2 = (aeqd && typeof aeqd === 'object' && 'default' in aeqd ? aeqd['default'] : aeqd);

	var vandg = createCommonjsModule(function (module, exports) {
	var adjust_lon = require$$2;
	var HALF_PI = Math.PI/2;
	var EPSLN = 1.0e-10;
	var asinz = require$$0$4;
	/* Initialize the Van Der Grinten projection
	  ----------------------------------------*/
	exports.init = function() {
	  //this.R = 6370997; //Radius of earth
	  this.R = this.a;
	};

	exports.forward = function(p) {

	  var lon = p.x;
	  var lat = p.y;

	  /* Forward equations
	    -----------------*/
	  var dlon = adjust_lon(lon - this.long0);
	  var x, y;

	  if (Math.abs(lat) <= EPSLN) {
	    x = this.x0 + this.R * dlon;
	    y = this.y0;
	  }
	  var theta = asinz(2 * Math.abs(lat / Math.PI));
	  if ((Math.abs(dlon) <= EPSLN) || (Math.abs(Math.abs(lat) - HALF_PI) <= EPSLN)) {
	    x = this.x0;
	    if (lat >= 0) {
	      y = this.y0 + Math.PI * this.R * Math.tan(0.5 * theta);
	    }
	    else {
	      y = this.y0 + Math.PI * this.R * -Math.tan(0.5 * theta);
	    }
	    //  return(OK);
	  }
	  var al = 0.5 * Math.abs((Math.PI / dlon) - (dlon / Math.PI));
	  var asq = al * al;
	  var sinth = Math.sin(theta);
	  var costh = Math.cos(theta);

	  var g = costh / (sinth + costh - 1);
	  var gsq = g * g;
	  var m = g * (2 / sinth - 1);
	  var msq = m * m;
	  var con = Math.PI * this.R * (al * (g - msq) + Math.sqrt(asq * (g - msq) * (g - msq) - (msq + asq) * (gsq - msq))) / (msq + asq);
	  if (dlon < 0) {
	    con = -con;
	  }
	  x = this.x0 + con;
	  //con = Math.abs(con / (Math.PI * this.R));
	  var q = asq + g;
	  con = Math.PI * this.R * (m * q - al * Math.sqrt((msq + asq) * (asq + 1) - q * q)) / (msq + asq);
	  if (lat >= 0) {
	    //y = this.y0 + Math.PI * this.R * Math.sqrt(1 - con * con - 2 * al * con);
	    y = this.y0 + con;
	  }
	  else {
	    //y = this.y0 - Math.PI * this.R * Math.sqrt(1 - con * con - 2 * al * con);
	    y = this.y0 - con;
	  }
	  p.x = x;
	  p.y = y;
	  return p;
	};

	/* Van Der Grinten inverse equations--mapping x,y to lat/long
	  ---------------------------------------------------------*/
	exports.inverse = function(p) {
	  var lon, lat;
	  var xx, yy, xys, c1, c2, c3;
	  var a1;
	  var m1;
	  var con;
	  var th1;
	  var d;

	  /* inverse equations
	    -----------------*/
	  p.x -= this.x0;
	  p.y -= this.y0;
	  con = Math.PI * this.R;
	  xx = p.x / con;
	  yy = p.y / con;
	  xys = xx * xx + yy * yy;
	  c1 = -Math.abs(yy) * (1 + xys);
	  c2 = c1 - 2 * yy * yy + xx * xx;
	  c3 = -2 * c1 + 1 + 2 * yy * yy + xys * xys;
	  d = yy * yy / c3 + (2 * c2 * c2 * c2 / c3 / c3 / c3 - 9 * c1 * c2 / c3 / c3) / 27;
	  a1 = (c1 - c2 * c2 / 3 / c3) / c3;
	  m1 = 2 * Math.sqrt(-a1 / 3);
	  con = ((3 * d) / a1) / m1;
	  if (Math.abs(con) > 1) {
	    if (con >= 0) {
	      con = 1;
	    }
	    else {
	      con = -1;
	    }
	  }
	  th1 = Math.acos(con) / 3;
	  if (p.y >= 0) {
	    lat = (-m1 * Math.cos(th1 + Math.PI / 3) - c2 / 3 / c3) * Math.PI;
	  }
	  else {
	    lat = -(-m1 * Math.cos(th1 + Math.PI / 3) - c2 / 3 / c3) * Math.PI;
	  }

	  if (Math.abs(xx) < EPSLN) {
	    lon = this.long0;
	  }
	  else {
	    lon = adjust_lon(this.long0 + Math.PI * (xys - 1 + Math.sqrt(1 + 2 * (xx * xx - yy * yy) + xys * xys)) / 2 / xx);
	  }

	  p.x = lon;
	  p.y = lat;
	  return p;
	};
	exports.names = ["Van_der_Grinten_I", "VanDerGrinten", "vandg"];
	});

	var require$$1$1 = (vandg && typeof vandg === 'object' && 'default' in vandg ? vandg['default'] : vandg);

	var adjust_lat = createCommonjsModule(function (module) {
	var HALF_PI = Math.PI/2;
	var sign = require$$1;

	module.exports = function(x) {
	  return (Math.abs(x) < HALF_PI) ? x : (x - (sign(x) * Math.PI));
	};
	});

	var require$$1$2 = (adjust_lat && typeof adjust_lat === 'object' && 'default' in adjust_lat ? adjust_lat['default'] : adjust_lat);

	var msfnz = createCommonjsModule(function (module) {
	module.exports = function(eccent, sinphi, cosphi) {
	  var con = eccent * sinphi;
	  return cosphi / (Math.sqrt(1 - con * con));
	};
	});

	var require$$3$2 = (msfnz && typeof msfnz === 'object' && 'default' in msfnz ? msfnz['default'] : msfnz);

	var eqdc = createCommonjsModule(function (module, exports) {
	var e0fn = require$$7;
	var e1fn = require$$6;
	var e2fn = require$$5;
	var e3fn = require$$4;
	var msfnz = require$$3$2;
	var mlfn = require$$3$1;
	var adjust_lon = require$$2;
	var adjust_lat = require$$1$2;
	var imlfn = require$$0$3;
	var EPSLN = 1.0e-10;
	exports.init = function() {

	  /* Place parameters in static storage for common use
	      -------------------------------------------------*/
	  // Standard Parallels cannot be equal and on opposite sides of the equator
	  if (Math.abs(this.lat1 + this.lat2) < EPSLN) {
	    return;
	  }
	  this.lat2 = this.lat2 || this.lat1;
	  this.temp = this.b / this.a;
	  this.es = 1 - Math.pow(this.temp, 2);
	  this.e = Math.sqrt(this.es);
	  this.e0 = e0fn(this.es);
	  this.e1 = e1fn(this.es);
	  this.e2 = e2fn(this.es);
	  this.e3 = e3fn(this.es);

	  this.sinphi = Math.sin(this.lat1);
	  this.cosphi = Math.cos(this.lat1);

	  this.ms1 = msfnz(this.e, this.sinphi, this.cosphi);
	  this.ml1 = mlfn(this.e0, this.e1, this.e2, this.e3, this.lat1);

	  if (Math.abs(this.lat1 - this.lat2) < EPSLN) {
	    this.ns = this.sinphi;
	  }
	  else {
	    this.sinphi = Math.sin(this.lat2);
	    this.cosphi = Math.cos(this.lat2);
	    this.ms2 = msfnz(this.e, this.sinphi, this.cosphi);
	    this.ml2 = mlfn(this.e0, this.e1, this.e2, this.e3, this.lat2);
	    this.ns = (this.ms1 - this.ms2) / (this.ml2 - this.ml1);
	  }
	  this.g = this.ml1 + this.ms1 / this.ns;
	  this.ml0 = mlfn(this.e0, this.e1, this.e2, this.e3, this.lat0);
	  this.rh = this.a * (this.g - this.ml0);
	};


	/* Equidistant Conic forward equations--mapping lat,long to x,y
	  -----------------------------------------------------------*/
	exports.forward = function(p) {
	  var lon = p.x;
	  var lat = p.y;
	  var rh1;

	  /* Forward equations
	      -----------------*/
	  if (this.sphere) {
	    rh1 = this.a * (this.g - lat);
	  }
	  else {
	    var ml = mlfn(this.e0, this.e1, this.e2, this.e3, lat);
	    rh1 = this.a * (this.g - ml);
	  }
	  var theta = this.ns * adjust_lon(lon - this.long0);
	  var x = this.x0 + rh1 * Math.sin(theta);
	  var y = this.y0 + this.rh - rh1 * Math.cos(theta);
	  p.x = x;
	  p.y = y;
	  return p;
	};

	/* Inverse equations
	  -----------------*/
	exports.inverse = function(p) {
	  p.x -= this.x0;
	  p.y = this.rh - p.y + this.y0;
	  var con, rh1, lat, lon;
	  if (this.ns >= 0) {
	    rh1 = Math.sqrt(p.x * p.x + p.y * p.y);
	    con = 1;
	  }
	  else {
	    rh1 = -Math.sqrt(p.x * p.x + p.y * p.y);
	    con = -1;
	  }
	  var theta = 0;
	  if (rh1 !== 0) {
	    theta = Math.atan2(con * p.x, con * p.y);
	  }

	  if (this.sphere) {
	    lon = adjust_lon(this.long0 + theta / this.ns);
	    lat = adjust_lat(this.g - rh1 / this.a);
	    p.x = lon;
	    p.y = lat;
	    return p;
	  }
	  else {
	    var ml = this.g - rh1 / this.a;
	    lat = imlfn(ml, this.e0, this.e1, this.e2, this.e3);
	    lon = adjust_lon(this.long0 + theta / this.ns);
	    p.x = lon;
	    p.y = lat;
	    return p;
	  }

	};
	exports.names = ["Equidistant_Conic", "eqdc"];
	});

	var require$$2$1 = (eqdc && typeof eqdc === 'object' && 'default' in eqdc ? eqdc['default'] : eqdc);

	var moll = createCommonjsModule(function (module, exports) {
	var adjust_lon = require$$2;
	var EPSLN = 1.0e-10;
	exports.init = function() {};

	/* Mollweide forward equations--mapping lat,long to x,y
	    ----------------------------------------------------*/
	exports.forward = function(p) {

	  /* Forward equations
	      -----------------*/
	  var lon = p.x;
	  var lat = p.y;

	  var delta_lon = adjust_lon(lon - this.long0);
	  var theta = lat;
	  var con = Math.PI * Math.sin(lat);

	  /* Iterate using the Newton-Raphson method to find theta
	      -----------------------------------------------------*/
	  for (var i = 0; true; i++) {
	    var delta_theta = -(theta + Math.sin(theta) - con) / (1 + Math.cos(theta));
	    theta += delta_theta;
	    if (Math.abs(delta_theta) < EPSLN) {
	      break;
	    }
	  }
	  theta /= 2;

	  /* If the latitude is 90 deg, force the x coordinate to be "0 + false easting"
	       this is done here because of precision problems with "cos(theta)"
	       --------------------------------------------------------------------------*/
	  if (Math.PI / 2 - Math.abs(lat) < EPSLN) {
	    delta_lon = 0;
	  }
	  var x = 0.900316316158 * this.a * delta_lon * Math.cos(theta) + this.x0;
	  var y = 1.4142135623731 * this.a * Math.sin(theta) + this.y0;

	  p.x = x;
	  p.y = y;
	  return p;
	};

	exports.inverse = function(p) {
	  var theta;
	  var arg;

	  /* Inverse equations
	      -----------------*/
	  p.x -= this.x0;
	  p.y -= this.y0;
	  arg = p.y / (1.4142135623731 * this.a);

	  /* Because of division by zero problems, 'arg' can not be 1.  Therefore
	       a number very close to one is used instead.
	       -------------------------------------------------------------------*/
	  if (Math.abs(arg) > 0.999999999999) {
	    arg = 0.999999999999;
	  }
	  theta = Math.asin(arg);
	  var lon = adjust_lon(this.long0 + (p.x / (0.900316316158 * this.a * Math.cos(theta))));
	  if (lon < (-Math.PI)) {
	    lon = -Math.PI;
	  }
	  if (lon > Math.PI) {
	    lon = Math.PI;
	  }
	  arg = (2 * theta + Math.sin(2 * theta)) / Math.PI;
	  if (Math.abs(arg) > 1) {
	    arg = 1;
	  }
	  var lat = Math.asin(arg);

	  p.x = lon;
	  p.y = lat;
	  return p;
	};
	exports.names = ["Mollweide", "moll"];
	});

	var require$$3$3 = (moll && typeof moll === 'object' && 'default' in moll ? moll['default'] : moll);

	var pj_mlfn = createCommonjsModule(function (module) {
	module.exports = function(phi, sphi, cphi, en) {
	  cphi *= sphi;
	  sphi *= sphi;
	  return (en[0] * phi - cphi * (en[1] + sphi * (en[2] + sphi * (en[3] + sphi * en[4]))));
	};
	});

	var require$$0$5 = (pj_mlfn && typeof pj_mlfn === 'object' && 'default' in pj_mlfn ? pj_mlfn['default'] : pj_mlfn);

	var pj_inv_mlfn = createCommonjsModule(function (module) {
	var pj_mlfn = require$$0$5;
	var EPSLN = 1.0e-10;
	var MAX_ITER = 20;
	module.exports = function(arg, es, en) {
	  var k = 1 / (1 - es);
	  var phi = arg;
	  for (var i = MAX_ITER; i; --i) { /* rarely goes over 2 iterations */
	    var s = Math.sin(phi);
	    var t = 1 - es * s * s;
	    //t = this.pj_mlfn(phi, s, Math.cos(phi), en) - arg;
	    //phi -= t * (t * Math.sqrt(t)) * k;
	    t = (pj_mlfn(phi, s, Math.cos(phi), en) - arg) * (t * Math.sqrt(t)) * k;
	    phi -= t;
	    if (Math.abs(t) < EPSLN) {
	      return phi;
	    }
	  }
	  //..reportError("cass:pj_inv_mlfn: Convergence error");
	  return phi;
	};
	});

	var require$$1$3 = (pj_inv_mlfn && typeof pj_inv_mlfn === 'object' && 'default' in pj_inv_mlfn ? pj_inv_mlfn['default'] : pj_inv_mlfn);

	var pj_enfn = createCommonjsModule(function (module) {
	var C00 = 1;
	var C02 = 0.25;
	var C04 = 0.046875;
	var C06 = 0.01953125;
	var C08 = 0.01068115234375;
	var C22 = 0.75;
	var C44 = 0.46875;
	var C46 = 0.01302083333333333333;
	var C48 = 0.00712076822916666666;
	var C66 = 0.36458333333333333333;
	var C68 = 0.00569661458333333333;
	var C88 = 0.3076171875;

	module.exports = function(es) {
	  var en = [];
	  en[0] = C00 - es * (C02 + es * (C04 + es * (C06 + es * C08)));
	  en[1] = es * (C22 - es * (C04 + es * (C06 + es * C08)));
	  var t = es * es;
	  en[2] = t * (C44 - es * (C46 + es * C48));
	  t *= es;
	  en[3] = t * (C66 - es * C68);
	  en[4] = t * es * C88;
	  return en;
	};
	});

	var require$$3$4 = (pj_enfn && typeof pj_enfn === 'object' && 'default' in pj_enfn ? pj_enfn['default'] : pj_enfn);

	var sinu = createCommonjsModule(function (module, exports) {
	var adjust_lon = require$$2;
	var adjust_lat = require$$1$2;
	var pj_enfn = require$$3$4;
	var MAX_ITER = 20;
	var pj_mlfn = require$$0$5;
	var pj_inv_mlfn = require$$1$3;
	var HALF_PI = Math.PI/2;
	var EPSLN = 1.0e-10;
	var asinz = require$$0$4;
	exports.init = function() {
	  /* Place parameters in static storage for common use
	    -------------------------------------------------*/


	  if (!this.sphere) {
	    this.en = pj_enfn(this.es);
	  }
	  else {
	    this.n = 1;
	    this.m = 0;
	    this.es = 0;
	    this.C_y = Math.sqrt((this.m + 1) / this.n);
	    this.C_x = this.C_y / (this.m + 1);
	  }

	};

	/* Sinusoidal forward equations--mapping lat,long to x,y
	  -----------------------------------------------------*/
	exports.forward = function(p) {
	  var x, y;
	  var lon = p.x;
	  var lat = p.y;
	  /* Forward equations
	    -----------------*/
	  lon = adjust_lon(lon - this.long0);

	  if (this.sphere) {
	    if (!this.m) {
	      lat = this.n !== 1 ? Math.asin(this.n * Math.sin(lat)) : lat;
	    }
	    else {
	      var k = this.n * Math.sin(lat);
	      for (var i = MAX_ITER; i; --i) {
	        var V = (this.m * lat + Math.sin(lat) - k) / (this.m + Math.cos(lat));
	        lat -= V;
	        if (Math.abs(V) < EPSLN) {
	          break;
	        }
	      }
	    }
	    x = this.a * this.C_x * lon * (this.m + Math.cos(lat));
	    y = this.a * this.C_y * lat;

	  }
	  else {

	    var s = Math.sin(lat);
	    var c = Math.cos(lat);
	    y = this.a * pj_mlfn(lat, s, c, this.en);
	    x = this.a * lon * c / Math.sqrt(1 - this.es * s * s);
	  }

	  p.x = x;
	  p.y = y;
	  return p;
	};

	exports.inverse = function(p) {
	  var lat, temp, lon, s;

	  p.x -= this.x0;
	  lon = p.x / this.a;
	  p.y -= this.y0;
	  lat = p.y / this.a;

	  if (this.sphere) {
	    lat /= this.C_y;
	    lon = lon / (this.C_x * (this.m + Math.cos(lat)));
	    if (this.m) {
	      lat = asinz((this.m * lat + Math.sin(lat)) / this.n);
	    }
	    else if (this.n !== 1) {
	      lat = asinz(Math.sin(lat) / this.n);
	    }
	    lon = adjust_lon(lon + this.long0);
	    lat = adjust_lat(lat);
	  }
	  else {
	    lat = pj_inv_mlfn(p.y / this.a, this.es, this.en);
	    s = Math.abs(lat);
	    if (s < HALF_PI) {
	      s = Math.sin(lat);
	      temp = this.long0 + p.x * Math.sqrt(1 - this.es * s * s) / (this.a * Math.cos(lat));
	      //temp = this.long0 + p.x / (this.a * Math.cos(lat));
	      lon = adjust_lon(temp);
	    }
	    else if ((s - EPSLN) < HALF_PI) {
	      lon = this.long0;
	    }
	  }
	  p.x = lon;
	  p.y = lat;
	  return p;
	};
	exports.names = ["Sinusoidal", "sinu"];
	});

	var require$$4$1 = (sinu && typeof sinu === 'object' && 'default' in sinu ? sinu['default'] : sinu);

	var mill = createCommonjsModule(function (module, exports) {
	var adjust_lon = require$$2;
	/*
	  reference
	    "New Equal-Area Map Projections for Noncircular Regions", John P. Snyder,
	    The American Cartographer, Vol 15, No. 4, October 1988, pp. 341-355.
	  */


	/* Initialize the Miller Cylindrical projection
	  -------------------------------------------*/
	exports.init = function() {
	  //no-op
	};


	/* Miller Cylindrical forward equations--mapping lat,long to x,y
	    ------------------------------------------------------------*/
	exports.forward = function(p) {
	  var lon = p.x;
	  var lat = p.y;
	  /* Forward equations
	      -----------------*/
	  var dlon = adjust_lon(lon - this.long0);
	  var x = this.x0 + this.a * dlon;
	  var y = this.y0 + this.a * Math.log(Math.tan((Math.PI / 4) + (lat / 2.5))) * 1.25;

	  p.x = x;
	  p.y = y;
	  return p;
	};

	/* Miller Cylindrical inverse equations--mapping x,y to lat/long
	    ------------------------------------------------------------*/
	exports.inverse = function(p) {
	  p.x -= this.x0;
	  p.y -= this.y0;

	  var lon = adjust_lon(this.long0 + p.x / this.a);
	  var lat = 2.5 * (Math.atan(Math.exp(0.8 * p.y / this.a)) - Math.PI / 4);

	  p.x = lon;
	  p.y = lat;
	  return p;
	};
	exports.names = ["Miller_Cylindrical", "mill"];
	});

	var require$$5$1 = (mill && typeof mill === 'object' && 'default' in mill ? mill['default'] : mill);

	var nzmg = createCommonjsModule(function (module, exports) {
	var SEC_TO_RAD = 4.84813681109535993589914102357e-6;
	/*
	  reference
	    Department of Land and Survey Technical Circular 1973/32
	      http://www.linz.govt.nz/docs/miscellaneous/nz-map-definition.pdf
	    OSG Technical Report 4.1
	      http://www.linz.govt.nz/docs/miscellaneous/nzmg.pdf
	  */

	/**
	 * iterations: Number of iterations to refine inverse transform.
	 *     0 -> km accuracy
	 *     1 -> m accuracy -- suitable for most mapping applications
	 *     2 -> mm accuracy
	 */
	exports.iterations = 1;

	exports.init = function() {
	  this.A = [];
	  this.A[1] = 0.6399175073;
	  this.A[2] = -0.1358797613;
	  this.A[3] = 0.063294409;
	  this.A[4] = -0.02526853;
	  this.A[5] = 0.0117879;
	  this.A[6] = -0.0055161;
	  this.A[7] = 0.0026906;
	  this.A[8] = -0.001333;
	  this.A[9] = 0.00067;
	  this.A[10] = -0.00034;

	  this.B_re = [];
	  this.B_im = [];
	  this.B_re[1] = 0.7557853228;
	  this.B_im[1] = 0;
	  this.B_re[2] = 0.249204646;
	  this.B_im[2] = 0.003371507;
	  this.B_re[3] = -0.001541739;
	  this.B_im[3] = 0.041058560;
	  this.B_re[4] = -0.10162907;
	  this.B_im[4] = 0.01727609;
	  this.B_re[5] = -0.26623489;
	  this.B_im[5] = -0.36249218;
	  this.B_re[6] = -0.6870983;
	  this.B_im[6] = -1.1651967;

	  this.C_re = [];
	  this.C_im = [];
	  this.C_re[1] = 1.3231270439;
	  this.C_im[1] = 0;
	  this.C_re[2] = -0.577245789;
	  this.C_im[2] = -0.007809598;
	  this.C_re[3] = 0.508307513;
	  this.C_im[3] = -0.112208952;
	  this.C_re[4] = -0.15094762;
	  this.C_im[4] = 0.18200602;
	  this.C_re[5] = 1.01418179;
	  this.C_im[5] = 1.64497696;
	  this.C_re[6] = 1.9660549;
	  this.C_im[6] = 2.5127645;

	  this.D = [];
	  this.D[1] = 1.5627014243;
	  this.D[2] = 0.5185406398;
	  this.D[3] = -0.03333098;
	  this.D[4] = -0.1052906;
	  this.D[5] = -0.0368594;
	  this.D[6] = 0.007317;
	  this.D[7] = 0.01220;
	  this.D[8] = 0.00394;
	  this.D[9] = -0.0013;
	};

	/**
	    New Zealand Map Grid Forward  - long/lat to x/y
	    long/lat in radians
	  */
	exports.forward = function(p) {
	  var n;
	  var lon = p.x;
	  var lat = p.y;

	  var delta_lat = lat - this.lat0;
	  var delta_lon = lon - this.long0;

	  // 1. Calculate d_phi and d_psi    ...                          // and d_lambda
	  // For this algorithm, delta_latitude is in seconds of arc x 10-5, so we need to scale to those units. Longitude is radians.
	  var d_phi = delta_lat / SEC_TO_RAD * 1E-5;
	  var d_lambda = delta_lon;
	  var d_phi_n = 1; // d_phi^0

	  var d_psi = 0;
	  for (n = 1; n <= 10; n++) {
	    d_phi_n = d_phi_n * d_phi;
	    d_psi = d_psi + this.A[n] * d_phi_n;
	  }

	  // 2. Calculate theta
	  var th_re = d_psi;
	  var th_im = d_lambda;

	  // 3. Calculate z
	  var th_n_re = 1;
	  var th_n_im = 0; // theta^0
	  var th_n_re1;
	  var th_n_im1;

	  var z_re = 0;
	  var z_im = 0;
	  for (n = 1; n <= 6; n++) {
	    th_n_re1 = th_n_re * th_re - th_n_im * th_im;
	    th_n_im1 = th_n_im * th_re + th_n_re * th_im;
	    th_n_re = th_n_re1;
	    th_n_im = th_n_im1;
	    z_re = z_re + this.B_re[n] * th_n_re - this.B_im[n] * th_n_im;
	    z_im = z_im + this.B_im[n] * th_n_re + this.B_re[n] * th_n_im;
	  }

	  // 4. Calculate easting and northing
	  p.x = (z_im * this.a) + this.x0;
	  p.y = (z_re * this.a) + this.y0;

	  return p;
	};


	/**
	    New Zealand Map Grid Inverse  -  x/y to long/lat
	  */
	exports.inverse = function(p) {
	  var n;
	  var x = p.x;
	  var y = p.y;

	  var delta_x = x - this.x0;
	  var delta_y = y - this.y0;

	  // 1. Calculate z
	  var z_re = delta_y / this.a;
	  var z_im = delta_x / this.a;

	  // 2a. Calculate theta - first approximation gives km accuracy
	  var z_n_re = 1;
	  var z_n_im = 0; // z^0
	  var z_n_re1;
	  var z_n_im1;

	  var th_re = 0;
	  var th_im = 0;
	  for (n = 1; n <= 6; n++) {
	    z_n_re1 = z_n_re * z_re - z_n_im * z_im;
	    z_n_im1 = z_n_im * z_re + z_n_re * z_im;
	    z_n_re = z_n_re1;
	    z_n_im = z_n_im1;
	    th_re = th_re + this.C_re[n] * z_n_re - this.C_im[n] * z_n_im;
	    th_im = th_im + this.C_im[n] * z_n_re + this.C_re[n] * z_n_im;
	  }

	  // 2b. Iterate to refine the accuracy of the calculation
	  //        0 iterations gives km accuracy
	  //        1 iteration gives m accuracy -- good enough for most mapping applications
	  //        2 iterations bives mm accuracy
	  for (var i = 0; i < this.iterations; i++) {
	    var th_n_re = th_re;
	    var th_n_im = th_im;
	    var th_n_re1;
	    var th_n_im1;

	    var num_re = z_re;
	    var num_im = z_im;
	    for (n = 2; n <= 6; n++) {
	      th_n_re1 = th_n_re * th_re - th_n_im * th_im;
	      th_n_im1 = th_n_im * th_re + th_n_re * th_im;
	      th_n_re = th_n_re1;
	      th_n_im = th_n_im1;
	      num_re = num_re + (n - 1) * (this.B_re[n] * th_n_re - this.B_im[n] * th_n_im);
	      num_im = num_im + (n - 1) * (this.B_im[n] * th_n_re + this.B_re[n] * th_n_im);
	    }

	    th_n_re = 1;
	    th_n_im = 0;
	    var den_re = this.B_re[1];
	    var den_im = this.B_im[1];
	    for (n = 2; n <= 6; n++) {
	      th_n_re1 = th_n_re * th_re - th_n_im * th_im;
	      th_n_im1 = th_n_im * th_re + th_n_re * th_im;
	      th_n_re = th_n_re1;
	      th_n_im = th_n_im1;
	      den_re = den_re + n * (this.B_re[n] * th_n_re - this.B_im[n] * th_n_im);
	      den_im = den_im + n * (this.B_im[n] * th_n_re + this.B_re[n] * th_n_im);
	    }

	    // Complex division
	    var den2 = den_re * den_re + den_im * den_im;
	    th_re = (num_re * den_re + num_im * den_im) / den2;
	    th_im = (num_im * den_re - num_re * den_im) / den2;
	  }

	  // 3. Calculate d_phi              ...                                    // and d_lambda
	  var d_psi = th_re;
	  var d_lambda = th_im;
	  var d_psi_n = 1; // d_psi^0

	  var d_phi = 0;
	  for (n = 1; n <= 9; n++) {
	    d_psi_n = d_psi_n * d_psi;
	    d_phi = d_phi + this.D[n] * d_psi_n;
	  }

	  // 4. Calculate latitude and longitude
	  // d_phi is calcuated in second of arc * 10^-5, so we need to scale back to radians. d_lambda is in radians.
	  var lat = this.lat0 + (d_phi * SEC_TO_RAD * 1E5);
	  var lon = this.long0 + d_lambda;

	  p.x = lon;
	  p.y = lat;

	  return p;
	};
	exports.names = ["New_Zealand_Map_Grid", "nzmg"];
	});

	var require$$6$1 = (nzmg && typeof nzmg === 'object' && 'default' in nzmg ? nzmg['default'] : nzmg);

	var poly = createCommonjsModule(function (module, exports) {
	var e0fn = require$$7;
	var e1fn = require$$6;
	var e2fn = require$$5;
	var e3fn = require$$4;
	var adjust_lon = require$$2;
	var adjust_lat = require$$1$2;
	var mlfn = require$$3$1;
	var EPSLN = 1.0e-10;
	var gN = require$$3;
	var MAX_ITER = 20;
	exports.init = function() {
	  /* Place parameters in static storage for common use
	      -------------------------------------------------*/
	  this.temp = this.b / this.a;
	  this.es = 1 - Math.pow(this.temp, 2); // devait etre dans tmerc.js mais n y est pas donc je commente sinon retour de valeurs nulles
	  this.e = Math.sqrt(this.es);
	  this.e0 = e0fn(this.es);
	  this.e1 = e1fn(this.es);
	  this.e2 = e2fn(this.es);
	  this.e3 = e3fn(this.es);
	  this.ml0 = this.a * mlfn(this.e0, this.e1, this.e2, this.e3, this.lat0); //si que des zeros le calcul ne se fait pas
	};


	/* Polyconic forward equations--mapping lat,long to x,y
	    ---------------------------------------------------*/
	exports.forward = function(p) {
	  var lon = p.x;
	  var lat = p.y;
	  var x, y, el;
	  var dlon = adjust_lon(lon - this.long0);
	  el = dlon * Math.sin(lat);
	  if (this.sphere) {
	    if (Math.abs(lat) <= EPSLN) {
	      x = this.a * dlon;
	      y = -1 * this.a * this.lat0;
	    }
	    else {
	      x = this.a * Math.sin(el) / Math.tan(lat);
	      y = this.a * (adjust_lat(lat - this.lat0) + (1 - Math.cos(el)) / Math.tan(lat));
	    }
	  }
	  else {
	    if (Math.abs(lat) <= EPSLN) {
	      x = this.a * dlon;
	      y = -1 * this.ml0;
	    }
	    else {
	      var nl = gN(this.a, this.e, Math.sin(lat)) / Math.tan(lat);
	      x = nl * Math.sin(el);
	      y = this.a * mlfn(this.e0, this.e1, this.e2, this.e3, lat) - this.ml0 + nl * (1 - Math.cos(el));
	    }

	  }
	  p.x = x + this.x0;
	  p.y = y + this.y0;
	  return p;
	};


	/* Inverse equations
	  -----------------*/
	exports.inverse = function(p) {
	  var lon, lat, x, y, i;
	  var al, bl;
	  var phi, dphi;
	  x = p.x - this.x0;
	  y = p.y - this.y0;

	  if (this.sphere) {
	    if (Math.abs(y + this.a * this.lat0) <= EPSLN) {
	      lon = adjust_lon(x / this.a + this.long0);
	      lat = 0;
	    }
	    else {
	      al = this.lat0 + y / this.a;
	      bl = x * x / this.a / this.a + al * al;
	      phi = al;
	      var tanphi;
	      for (i = MAX_ITER; i; --i) {
	        tanphi = Math.tan(phi);
	        dphi = -1 * (al * (phi * tanphi + 1) - phi - 0.5 * (phi * phi + bl) * tanphi) / ((phi - al) / tanphi - 1);
	        phi += dphi;
	        if (Math.abs(dphi) <= EPSLN) {
	          lat = phi;
	          break;
	        }
	      }
	      lon = adjust_lon(this.long0 + (Math.asin(x * Math.tan(phi) / this.a)) / Math.sin(lat));
	    }
	  }
	  else {
	    if (Math.abs(y + this.ml0) <= EPSLN) {
	      lat = 0;
	      lon = adjust_lon(this.long0 + x / this.a);
	    }
	    else {

	      al = (this.ml0 + y) / this.a;
	      bl = x * x / this.a / this.a + al * al;
	      phi = al;
	      var cl, mln, mlnp, ma;
	      var con;
	      for (i = MAX_ITER; i; --i) {
	        con = this.e * Math.sin(phi);
	        cl = Math.sqrt(1 - con * con) * Math.tan(phi);
	        mln = this.a * mlfn(this.e0, this.e1, this.e2, this.e3, phi);
	        mlnp = this.e0 - 2 * this.e1 * Math.cos(2 * phi) + 4 * this.e2 * Math.cos(4 * phi) - 6 * this.e3 * Math.cos(6 * phi);
	        ma = mln / this.a;
	        dphi = (al * (cl * ma + 1) - ma - 0.5 * cl * (ma * ma + bl)) / (this.es * Math.sin(2 * phi) * (ma * ma + bl - 2 * al * ma) / (4 * cl) + (al - ma) * (cl * mlnp - 2 / Math.sin(2 * phi)) - mlnp);
	        phi -= dphi;
	        if (Math.abs(dphi) <= EPSLN) {
	          lat = phi;
	          break;
	        }
	      }

	      //lat=phi4z(this.e,this.e0,this.e1,this.e2,this.e3,al,bl,0,0);
	      cl = Math.sqrt(1 - this.es * Math.pow(Math.sin(lat), 2)) * Math.tan(lat);
	      lon = adjust_lon(this.long0 + Math.asin(x * cl / this.a) / Math.sin(lat));
	    }
	  }

	  p.x = lon;
	  p.y = lat;
	  return p;
	};
	exports.names = ["Polyconic", "poly"];
	});

	var require$$7$1 = (poly && typeof poly === 'object' && 'default' in poly ? poly['default'] : poly);

	var eqc = createCommonjsModule(function (module, exports) {
	var adjust_lon = require$$2;
	var adjust_lat = require$$1$2;
	exports.init = function() {

	  this.x0 = this.x0 || 0;
	  this.y0 = this.y0 || 0;
	  this.lat0 = this.lat0 || 0;
	  this.long0 = this.long0 || 0;
	  this.lat_ts = this.lat_ts || 0;
	  this.title = this.title || "Equidistant Cylindrical (Plate Carre)";

	  this.rc = Math.cos(this.lat_ts);
	};


	// forward equations--mapping lat,long to x,y
	// -----------------------------------------------------------------
	exports.forward = function(p) {

	  var lon = p.x;
	  var lat = p.y;

	  var dlon = adjust_lon(lon - this.long0);
	  var dlat = adjust_lat(lat - this.lat0);
	  p.x = this.x0 + (this.a * dlon * this.rc);
	  p.y = this.y0 + (this.a * dlat);
	  return p;
	};

	// inverse equations--mapping x,y to lat/long
	// -----------------------------------------------------------------
	exports.inverse = function(p) {

	  var x = p.x;
	  var y = p.y;

	  p.x = adjust_lon(this.long0 + ((x - this.x0) / (this.a * this.rc)));
	  p.y = adjust_lat(this.lat0 + ((y - this.y0) / (this.a)));
	  return p;
	};
	exports.names = ["Equirectangular", "Equidistant_Cylindrical", "eqc"];
	});

	var require$$8 = (eqc && typeof eqc === 'object' && 'default' in eqc ? eqc['default'] : eqc);

	var iqsfnz = createCommonjsModule(function (module) {
	var HALF_PI = Math.PI/2;

	module.exports = function(eccent, q) {
	  var temp = 1 - (1 - eccent * eccent) / (2 * eccent) * Math.log((1 - eccent) / (1 + eccent));
	  if (Math.abs(Math.abs(q) - temp) < 1.0E-6) {
	    if (q < 0) {
	      return (-1 * HALF_PI);
	    }
	    else {
	      return HALF_PI;
	    }
	  }
	  //var phi = 0.5* q/(1-eccent*eccent);
	  var phi = Math.asin(0.5 * q);
	  var dphi;
	  var sin_phi;
	  var cos_phi;
	  var con;
	  for (var i = 0; i < 30; i++) {
	    sin_phi = Math.sin(phi);
	    cos_phi = Math.cos(phi);
	    con = eccent * sin_phi;
	    dphi = Math.pow(1 - con * con, 2) / (2 * cos_phi) * (q / (1 - eccent * eccent) - sin_phi / (1 - con * con) + 0.5 / eccent * Math.log((1 - con) / (1 + con)));
	    phi += dphi;
	    if (Math.abs(dphi) <= 0.0000000001) {
	      return phi;
	    }
	  }

	  //console.log("IQSFN-CONV:Latitude failed to converge after 30 iterations");
	  return NaN;
	};
	});

	var require$$0$6 = (iqsfnz && typeof iqsfnz === 'object' && 'default' in iqsfnz ? iqsfnz['default'] : iqsfnz);

	var qsfnz = createCommonjsModule(function (module) {
	module.exports = function(eccent, sinphi) {
	  var con;
	  if (eccent > 1.0e-7) {
	    con = eccent * sinphi;
	    return ((1 - eccent * eccent) * (sinphi / (1 - con * con) - (0.5 / eccent) * Math.log((1 - con) / (1 + con))));
	  }
	  else {
	    return (2 * sinphi);
	  }
	};
	});

	var require$$1$4 = (qsfnz && typeof qsfnz === 'object' && 'default' in qsfnz ? qsfnz['default'] : qsfnz);

	var cea = createCommonjsModule(function (module, exports) {
	var adjust_lon = require$$2;
	var qsfnz = require$$1$4;
	var msfnz = require$$3$2;
	var iqsfnz = require$$0$6;
	/*
	  reference:  
	    "Cartographic Projection Procedures for the UNIX Environment-
	    A User's Manual" by Gerald I. Evenden,
	    USGS Open File Report 90-284and Release 4 Interim Reports (2003)
	*/
	exports.init = function() {
	  //no-op
	  if (!this.sphere) {
	    this.k0 = msfnz(this.e, Math.sin(this.lat_ts), Math.cos(this.lat_ts));
	  }
	};


	/* Cylindrical Equal Area forward equations--mapping lat,long to x,y
	    ------------------------------------------------------------*/
	exports.forward = function(p) {
	  var lon = p.x;
	  var lat = p.y;
	  var x, y;
	  /* Forward equations
	      -----------------*/
	  var dlon = adjust_lon(lon - this.long0);
	  if (this.sphere) {
	    x = this.x0 + this.a * dlon * Math.cos(this.lat_ts);
	    y = this.y0 + this.a * Math.sin(lat) / Math.cos(this.lat_ts);
	  }
	  else {
	    var qs = qsfnz(this.e, Math.sin(lat));
	    x = this.x0 + this.a * this.k0 * dlon;
	    y = this.y0 + this.a * qs * 0.5 / this.k0;
	  }

	  p.x = x;
	  p.y = y;
	  return p;
	};

	/* Cylindrical Equal Area inverse equations--mapping x,y to lat/long
	    ------------------------------------------------------------*/
	exports.inverse = function(p) {
	  p.x -= this.x0;
	  p.y -= this.y0;
	  var lon, lat;

	  if (this.sphere) {
	    lon = adjust_lon(this.long0 + (p.x / this.a) / Math.cos(this.lat_ts));
	    lat = Math.asin((p.y / this.a) * Math.cos(this.lat_ts));
	  }
	  else {
	    lat = iqsfnz(this.e, 2 * p.y * this.k0 / this.a);
	    lon = adjust_lon(this.long0 + p.x / (this.a * this.k0));
	  }

	  p.x = lon;
	  p.y = lat;
	  return p;
	};
	exports.names = ["cea"];
	});

	var require$$9 = (cea && typeof cea === 'object' && 'default' in cea ? cea['default'] : cea);

	var gnom = createCommonjsModule(function (module, exports) {
	var adjust_lon = require$$2;
	var EPSLN = 1.0e-10;
	var asinz = require$$0$4;

	/*
	  reference:
	    Wolfram Mathworld "Gnomonic Projection"
	    http://mathworld.wolfram.com/GnomonicProjection.html
	    Accessed: 12th November 2009
	  */
	exports.init = function() {

	  /* Place parameters in static storage for common use
	      -------------------------------------------------*/
	  this.sin_p14 = Math.sin(this.lat0);
	  this.cos_p14 = Math.cos(this.lat0);
	  // Approximation for projecting points to the horizon (infinity)
	  this.infinity_dist = 1000 * this.a;
	  this.rc = 1;
	};


	/* Gnomonic forward equations--mapping lat,long to x,y
	    ---------------------------------------------------*/
	exports.forward = function(p) {
	  var sinphi, cosphi; /* sin and cos value        */
	  var dlon; /* delta longitude value      */
	  var coslon; /* cos of longitude        */
	  var ksp; /* scale factor          */
	  var g;
	  var x, y;
	  var lon = p.x;
	  var lat = p.y;
	  /* Forward equations
	      -----------------*/
	  dlon = adjust_lon(lon - this.long0);

	  sinphi = Math.sin(lat);
	  cosphi = Math.cos(lat);

	  coslon = Math.cos(dlon);
	  g = this.sin_p14 * sinphi + this.cos_p14 * cosphi * coslon;
	  ksp = 1;
	  if ((g > 0) || (Math.abs(g) <= EPSLN)) {
	    x = this.x0 + this.a * ksp * cosphi * Math.sin(dlon) / g;
	    y = this.y0 + this.a * ksp * (this.cos_p14 * sinphi - this.sin_p14 * cosphi * coslon) / g;
	  }
	  else {

	    // Point is in the opposing hemisphere and is unprojectable
	    // We still need to return a reasonable point, so we project 
	    // to infinity, on a bearing 
	    // equivalent to the northern hemisphere equivalent
	    // This is a reasonable approximation for short shapes and lines that 
	    // straddle the horizon.

	    x = this.x0 + this.infinity_dist * cosphi * Math.sin(dlon);
	    y = this.y0 + this.infinity_dist * (this.cos_p14 * sinphi - this.sin_p14 * cosphi * coslon);

	  }
	  p.x = x;
	  p.y = y;
	  return p;
	};


	exports.inverse = function(p) {
	  var rh; /* Rho */
	  var sinc, cosc;
	  var c;
	  var lon, lat;

	  /* Inverse equations
	      -----------------*/
	  p.x = (p.x - this.x0) / this.a;
	  p.y = (p.y - this.y0) / this.a;

	  p.x /= this.k0;
	  p.y /= this.k0;

	  if ((rh = Math.sqrt(p.x * p.x + p.y * p.y))) {
	    c = Math.atan2(rh, this.rc);
	    sinc = Math.sin(c);
	    cosc = Math.cos(c);

	    lat = asinz(cosc * this.sin_p14 + (p.y * sinc * this.cos_p14) / rh);
	    lon = Math.atan2(p.x * sinc, rh * this.cos_p14 * cosc - p.y * this.sin_p14 * sinc);
	    lon = adjust_lon(this.long0 + lon);
	  }
	  else {
	    lat = this.phic0;
	    lon = 0;
	  }

	  p.x = lon;
	  p.y = lat;
	  return p;
	};
	exports.names = ["gnom"];
	});

	var require$$10 = (gnom && typeof gnom === 'object' && 'default' in gnom ? gnom['default'] : gnom);

	var aea = createCommonjsModule(function (module, exports) {
	var EPSLN = 1.0e-10;
	var msfnz = require$$3$2;
	var qsfnz = require$$1$4;
	var adjust_lon = require$$2;
	var asinz = require$$0$4;
	exports.init = function() {

	  if (Math.abs(this.lat1 + this.lat2) < EPSLN) {
	    return;
	  }
	  this.temp = this.b / this.a;
	  this.es = 1 - Math.pow(this.temp, 2);
	  this.e3 = Math.sqrt(this.es);

	  this.sin_po = Math.sin(this.lat1);
	  this.cos_po = Math.cos(this.lat1);
	  this.t1 = this.sin_po;
	  this.con = this.sin_po;
	  this.ms1 = msfnz(this.e3, this.sin_po, this.cos_po);
	  this.qs1 = qsfnz(this.e3, this.sin_po, this.cos_po);

	  this.sin_po = Math.sin(this.lat2);
	  this.cos_po = Math.cos(this.lat2);
	  this.t2 = this.sin_po;
	  this.ms2 = msfnz(this.e3, this.sin_po, this.cos_po);
	  this.qs2 = qsfnz(this.e3, this.sin_po, this.cos_po);

	  this.sin_po = Math.sin(this.lat0);
	  this.cos_po = Math.cos(this.lat0);
	  this.t3 = this.sin_po;
	  this.qs0 = qsfnz(this.e3, this.sin_po, this.cos_po);

	  if (Math.abs(this.lat1 - this.lat2) > EPSLN) {
	    this.ns0 = (this.ms1 * this.ms1 - this.ms2 * this.ms2) / (this.qs2 - this.qs1);
	  }
	  else {
	    this.ns0 = this.con;
	  }
	  this.c = this.ms1 * this.ms1 + this.ns0 * this.qs1;
	  this.rh = this.a * Math.sqrt(this.c - this.ns0 * this.qs0) / this.ns0;
	};

	/* Albers Conical Equal Area forward equations--mapping lat,long to x,y
	  -------------------------------------------------------------------*/
	exports.forward = function(p) {

	  var lon = p.x;
	  var lat = p.y;

	  this.sin_phi = Math.sin(lat);
	  this.cos_phi = Math.cos(lat);

	  var qs = qsfnz(this.e3, this.sin_phi, this.cos_phi);
	  var rh1 = this.a * Math.sqrt(this.c - this.ns0 * qs) / this.ns0;
	  var theta = this.ns0 * adjust_lon(lon - this.long0);
	  var x = rh1 * Math.sin(theta) + this.x0;
	  var y = this.rh - rh1 * Math.cos(theta) + this.y0;

	  p.x = x;
	  p.y = y;
	  return p;
	};


	exports.inverse = function(p) {
	  var rh1, qs, con, theta, lon, lat;

	  p.x -= this.x0;
	  p.y = this.rh - p.y + this.y0;
	  if (this.ns0 >= 0) {
	    rh1 = Math.sqrt(p.x * p.x + p.y * p.y);
	    con = 1;
	  }
	  else {
	    rh1 = -Math.sqrt(p.x * p.x + p.y * p.y);
	    con = -1;
	  }
	  theta = 0;
	  if (rh1 !== 0) {
	    theta = Math.atan2(con * p.x, con * p.y);
	  }
	  con = rh1 * this.ns0 / this.a;
	  if (this.sphere) {
	    lat = Math.asin((this.c - con * con) / (2 * this.ns0));
	  }
	  else {
	    qs = (this.c - con * con) / this.ns0;
	    lat = this.phi1z(this.e3, qs);
	  }

	  lon = adjust_lon(theta / this.ns0 + this.long0);
	  p.x = lon;
	  p.y = lat;
	  return p;
	};

	/* Function to compute phi1, the latitude for the inverse of the
	   Albers Conical Equal-Area projection.
	-------------------------------------------*/
	exports.phi1z = function(eccent, qs) {
	  var sinphi, cosphi, con, com, dphi;
	  var phi = asinz(0.5 * qs);
	  if (eccent < EPSLN) {
	    return phi;
	  }

	  var eccnts = eccent * eccent;
	  for (var i = 1; i <= 25; i++) {
	    sinphi = Math.sin(phi);
	    cosphi = Math.cos(phi);
	    con = eccent * sinphi;
	    com = 1 - con * con;
	    dphi = 0.5 * com * com / cosphi * (qs / (1 - eccnts) - sinphi / com + 0.5 / eccent * Math.log((1 - con) / (1 + con)));
	    phi = phi + dphi;
	    if (Math.abs(dphi) <= 1e-7) {
	      return phi;
	    }
	  }
	  return null;
	};
	exports.names = ["Albers_Conic_Equal_Area", "Albers", "aea"];
	});

	var require$$11 = (aea && typeof aea === 'object' && 'default' in aea ? aea['default'] : aea);

	var laea = createCommonjsModule(function (module, exports) {
	var HALF_PI = Math.PI/2;
	var FORTPI = Math.PI/4;
	var EPSLN = 1.0e-10;
	var qsfnz = require$$1$4;
	var adjust_lon = require$$2;
	/*
	  reference
	    "New Equal-Area Map Projections for Noncircular Regions", John P. Snyder,
	    The American Cartographer, Vol 15, No. 4, October 1988, pp. 341-355.
	  */

	exports.S_POLE = 1;
	exports.N_POLE = 2;
	exports.EQUIT = 3;
	exports.OBLIQ = 4;


	/* Initialize the Lambert Azimuthal Equal Area projection
	  ------------------------------------------------------*/
	exports.init = function() {
	  var t = Math.abs(this.lat0);
	  if (Math.abs(t - HALF_PI) < EPSLN) {
	    this.mode = this.lat0 < 0 ? this.S_POLE : this.N_POLE;
	  }
	  else if (Math.abs(t) < EPSLN) {
	    this.mode = this.EQUIT;
	  }
	  else {
	    this.mode = this.OBLIQ;
	  }
	  if (this.es > 0) {
	    var sinphi;

	    this.qp = qsfnz(this.e, 1);
	    this.mmf = 0.5 / (1 - this.es);
	    this.apa = this.authset(this.es);
	    switch (this.mode) {
	    case this.N_POLE:
	      this.dd = 1;
	      break;
	    case this.S_POLE:
	      this.dd = 1;
	      break;
	    case this.EQUIT:
	      this.rq = Math.sqrt(0.5 * this.qp);
	      this.dd = 1 / this.rq;
	      this.xmf = 1;
	      this.ymf = 0.5 * this.qp;
	      break;
	    case this.OBLIQ:
	      this.rq = Math.sqrt(0.5 * this.qp);
	      sinphi = Math.sin(this.lat0);
	      this.sinb1 = qsfnz(this.e, sinphi) / this.qp;
	      this.cosb1 = Math.sqrt(1 - this.sinb1 * this.sinb1);
	      this.dd = Math.cos(this.lat0) / (Math.sqrt(1 - this.es * sinphi * sinphi) * this.rq * this.cosb1);
	      this.ymf = (this.xmf = this.rq) / this.dd;
	      this.xmf *= this.dd;
	      break;
	    }
	  }
	  else {
	    if (this.mode === this.OBLIQ) {
	      this.sinph0 = Math.sin(this.lat0);
	      this.cosph0 = Math.cos(this.lat0);
	    }
	  }
	};

	/* Lambert Azimuthal Equal Area forward equations--mapping lat,long to x,y
	  -----------------------------------------------------------------------*/
	exports.forward = function(p) {

	  /* Forward equations
	      -----------------*/
	  var x, y, coslam, sinlam, sinphi, q, sinb, cosb, b, cosphi;
	  var lam = p.x;
	  var phi = p.y;

	  lam = adjust_lon(lam - this.long0);

	  if (this.sphere) {
	    sinphi = Math.sin(phi);
	    cosphi = Math.cos(phi);
	    coslam = Math.cos(lam);
	    if (this.mode === this.OBLIQ || this.mode === this.EQUIT) {
	      y = (this.mode === this.EQUIT) ? 1 + cosphi * coslam : 1 + this.sinph0 * sinphi + this.cosph0 * cosphi * coslam;
	      if (y <= EPSLN) {
	        return null;
	      }
	      y = Math.sqrt(2 / y);
	      x = y * cosphi * Math.sin(lam);
	      y *= (this.mode === this.EQUIT) ? sinphi : this.cosph0 * sinphi - this.sinph0 * cosphi * coslam;
	    }
	    else if (this.mode === this.N_POLE || this.mode === this.S_POLE) {
	      if (this.mode === this.N_POLE) {
	        coslam = -coslam;
	      }
	      if (Math.abs(phi + this.phi0) < EPSLN) {
	        return null;
	      }
	      y = FORTPI - phi * 0.5;
	      y = 2 * ((this.mode === this.S_POLE) ? Math.cos(y) : Math.sin(y));
	      x = y * Math.sin(lam);
	      y *= coslam;
	    }
	  }
	  else {
	    sinb = 0;
	    cosb = 0;
	    b = 0;
	    coslam = Math.cos(lam);
	    sinlam = Math.sin(lam);
	    sinphi = Math.sin(phi);
	    q = qsfnz(this.e, sinphi);
	    if (this.mode === this.OBLIQ || this.mode === this.EQUIT) {
	      sinb = q / this.qp;
	      cosb = Math.sqrt(1 - sinb * sinb);
	    }
	    switch (this.mode) {
	    case this.OBLIQ:
	      b = 1 + this.sinb1 * sinb + this.cosb1 * cosb * coslam;
	      break;
	    case this.EQUIT:
	      b = 1 + cosb * coslam;
	      break;
	    case this.N_POLE:
	      b = HALF_PI + phi;
	      q = this.qp - q;
	      break;
	    case this.S_POLE:
	      b = phi - HALF_PI;
	      q = this.qp + q;
	      break;
	    }
	    if (Math.abs(b) < EPSLN) {
	      return null;
	    }
	    switch (this.mode) {
	    case this.OBLIQ:
	    case this.EQUIT:
	      b = Math.sqrt(2 / b);
	      if (this.mode === this.OBLIQ) {
	        y = this.ymf * b * (this.cosb1 * sinb - this.sinb1 * cosb * coslam);
	      }
	      else {
	        y = (b = Math.sqrt(2 / (1 + cosb * coslam))) * sinb * this.ymf;
	      }
	      x = this.xmf * b * cosb * sinlam;
	      break;
	    case this.N_POLE:
	    case this.S_POLE:
	      if (q >= 0) {
	        x = (b = Math.sqrt(q)) * sinlam;
	        y = coslam * ((this.mode === this.S_POLE) ? b : -b);
	      }
	      else {
	        x = y = 0;
	      }
	      break;
	    }
	  }

	  p.x = this.a * x + this.x0;
	  p.y = this.a * y + this.y0;
	  return p;
	};

	/* Inverse equations
	  -----------------*/
	exports.inverse = function(p) {
	  p.x -= this.x0;
	  p.y -= this.y0;
	  var x = p.x / this.a;
	  var y = p.y / this.a;
	  var lam, phi, cCe, sCe, q, rho, ab;

	  if (this.sphere) {
	    var cosz = 0,
	      rh, sinz = 0;

	    rh = Math.sqrt(x * x + y * y);
	    phi = rh * 0.5;
	    if (phi > 1) {
	      return null;
	    }
	    phi = 2 * Math.asin(phi);
	    if (this.mode === this.OBLIQ || this.mode === this.EQUIT) {
	      sinz = Math.sin(phi);
	      cosz = Math.cos(phi);
	    }
	    switch (this.mode) {
	    case this.EQUIT:
	      phi = (Math.abs(rh) <= EPSLN) ? 0 : Math.asin(y * sinz / rh);
	      x *= sinz;
	      y = cosz * rh;
	      break;
	    case this.OBLIQ:
	      phi = (Math.abs(rh) <= EPSLN) ? this.phi0 : Math.asin(cosz * this.sinph0 + y * sinz * this.cosph0 / rh);
	      x *= sinz * this.cosph0;
	      y = (cosz - Math.sin(phi) * this.sinph0) * rh;
	      break;
	    case this.N_POLE:
	      y = -y;
	      phi = HALF_PI - phi;
	      break;
	    case this.S_POLE:
	      phi -= HALF_PI;
	      break;
	    }
	    lam = (y === 0 && (this.mode === this.EQUIT || this.mode === this.OBLIQ)) ? 0 : Math.atan2(x, y);
	  }
	  else {
	    ab = 0;
	    if (this.mode === this.OBLIQ || this.mode === this.EQUIT) {
	      x /= this.dd;
	      y *= this.dd;
	      rho = Math.sqrt(x * x + y * y);
	      if (rho < EPSLN) {
	        p.x = 0;
	        p.y = this.phi0;
	        return p;
	      }
	      sCe = 2 * Math.asin(0.5 * rho / this.rq);
	      cCe = Math.cos(sCe);
	      x *= (sCe = Math.sin(sCe));
	      if (this.mode === this.OBLIQ) {
	        ab = cCe * this.sinb1 + y * sCe * this.cosb1 / rho;
	        q = this.qp * ab;
	        y = rho * this.cosb1 * cCe - y * this.sinb1 * sCe;
	      }
	      else {
	        ab = y * sCe / rho;
	        q = this.qp * ab;
	        y = rho * cCe;
	      }
	    }
	    else if (this.mode === this.N_POLE || this.mode === this.S_POLE) {
	      if (this.mode === this.N_POLE) {
	        y = -y;
	      }
	      q = (x * x + y * y);
	      if (!q) {
	        p.x = 0;
	        p.y = this.phi0;
	        return p;
	      }
	      ab = 1 - q / this.qp;
	      if (this.mode === this.S_POLE) {
	        ab = -ab;
	      }
	    }
	    lam = Math.atan2(x, y);
	    phi = this.authlat(Math.asin(ab), this.apa);
	  }


	  p.x = adjust_lon(this.long0 + lam);
	  p.y = phi;
	  return p;
	};

	/* determine latitude from authalic latitude */
	exports.P00 = 0.33333333333333333333;
	exports.P01 = 0.17222222222222222222;
	exports.P02 = 0.10257936507936507936;
	exports.P10 = 0.06388888888888888888;
	exports.P11 = 0.06640211640211640211;
	exports.P20 = 0.01641501294219154443;

	exports.authset = function(es) {
	  var t;
	  var APA = [];
	  APA[0] = es * this.P00;
	  t = es * es;
	  APA[0] += t * this.P01;
	  APA[1] = t * this.P10;
	  t *= es;
	  APA[0] += t * this.P02;
	  APA[1] += t * this.P11;
	  APA[2] = t * this.P20;
	  return APA;
	};

	exports.authlat = function(beta, APA) {
	  var t = beta + beta;
	  return (beta + APA[0] * Math.sin(t) + APA[1] * Math.sin(t + t) + APA[2] * Math.sin(t + t + t));
	};
	exports.names = ["Lambert Azimuthal Equal Area", "Lambert_Azimuthal_Equal_Area", "laea"];
	});

	var require$$12 = (laea && typeof laea === 'object' && 'default' in laea ? laea['default'] : laea);

	var cass = createCommonjsModule(function (module, exports) {
	var mlfn = require$$3$1;
	var e0fn = require$$7;
	var e1fn = require$$6;
	var e2fn = require$$5;
	var e3fn = require$$4;
	var gN = require$$3;
	var adjust_lon = require$$2;
	var adjust_lat = require$$1$2;
	var imlfn = require$$0$3;
	var HALF_PI = Math.PI/2;
	var EPSLN = 1.0e-10;
	exports.init = function() {
	  if (!this.sphere) {
	    this.e0 = e0fn(this.es);
	    this.e1 = e1fn(this.es);
	    this.e2 = e2fn(this.es);
	    this.e3 = e3fn(this.es);
	    this.ml0 = this.a * mlfn(this.e0, this.e1, this.e2, this.e3, this.lat0);
	  }
	};



	/* Cassini forward equations--mapping lat,long to x,y
	  -----------------------------------------------------------------------*/
	exports.forward = function(p) {

	  /* Forward equations
	      -----------------*/
	  var x, y;
	  var lam = p.x;
	  var phi = p.y;
	  lam = adjust_lon(lam - this.long0);

	  if (this.sphere) {
	    x = this.a * Math.asin(Math.cos(phi) * Math.sin(lam));
	    y = this.a * (Math.atan2(Math.tan(phi), Math.cos(lam)) - this.lat0);
	  }
	  else {
	    //ellipsoid
	    var sinphi = Math.sin(phi);
	    var cosphi = Math.cos(phi);
	    var nl = gN(this.a, this.e, sinphi);
	    var tl = Math.tan(phi) * Math.tan(phi);
	    var al = lam * Math.cos(phi);
	    var asq = al * al;
	    var cl = this.es * cosphi * cosphi / (1 - this.es);
	    var ml = this.a * mlfn(this.e0, this.e1, this.e2, this.e3, phi);

	    x = nl * al * (1 - asq * tl * (1 / 6 - (8 - tl + 8 * cl) * asq / 120));
	    y = ml - this.ml0 + nl * sinphi / cosphi * asq * (0.5 + (5 - tl + 6 * cl) * asq / 24);


	  }

	  p.x = x + this.x0;
	  p.y = y + this.y0;
	  return p;
	};

	/* Inverse equations
	  -----------------*/
	exports.inverse = function(p) {
	  p.x -= this.x0;
	  p.y -= this.y0;
	  var x = p.x / this.a;
	  var y = p.y / this.a;
	  var phi, lam;

	  if (this.sphere) {
	    var dd = y + this.lat0;
	    phi = Math.asin(Math.sin(dd) * Math.cos(x));
	    lam = Math.atan2(Math.tan(x), Math.cos(dd));
	  }
	  else {
	    /* ellipsoid */
	    var ml1 = this.ml0 / this.a + y;
	    var phi1 = imlfn(ml1, this.e0, this.e1, this.e2, this.e3);
	    if (Math.abs(Math.abs(phi1) - HALF_PI) <= EPSLN) {
	      p.x = this.long0;
	      p.y = HALF_PI;
	      if (y < 0) {
	        p.y *= -1;
	      }
	      return p;
	    }
	    var nl1 = gN(this.a, this.e, Math.sin(phi1));

	    var rl1 = nl1 * nl1 * nl1 / this.a / this.a * (1 - this.es);
	    var tl1 = Math.pow(Math.tan(phi1), 2);
	    var dl = x * this.a / nl1;
	    var dsq = dl * dl;
	    phi = phi1 - nl1 * Math.tan(phi1) / rl1 * dl * dl * (0.5 - (1 + 3 * tl1) * dl * dl / 24);
	    lam = dl * (1 - dsq * (tl1 / 3 + (1 + 3 * tl1) * tl1 * dsq / 15)) / Math.cos(phi1);

	  }

	  p.x = adjust_lon(lam + this.long0);
	  p.y = adjust_lat(phi);
	  return p;

	};
	exports.names = ["Cassini", "Cassini_Soldner", "cass"];
	});

	var require$$13 = (cass && typeof cass === 'object' && 'default' in cass ? cass['default'] : cass);

	var krovak = createCommonjsModule(function (module, exports) {
	var adjust_lon = require$$2;
	exports.init = function() {
	  this.a = 6377397.155;
	  this.es = 0.006674372230614;
	  this.e = Math.sqrt(this.es);
	  if (!this.lat0) {
	    this.lat0 = 0.863937979737193;
	  }
	  if (!this.long0) {
	    this.long0 = 0.7417649320975901 - 0.308341501185665;
	  }
	  /* if scale not set default to 0.9999 */
	  if (!this.k0) {
	    this.k0 = 0.9999;
	  }
	  this.s45 = 0.785398163397448; /* 45 */
	  this.s90 = 2 * this.s45;
	  this.fi0 = this.lat0;
	  this.e2 = this.es;
	  this.e = Math.sqrt(this.e2);
	  this.alfa = Math.sqrt(1 + (this.e2 * Math.pow(Math.cos(this.fi0), 4)) / (1 - this.e2));
	  this.uq = 1.04216856380474;
	  this.u0 = Math.asin(Math.sin(this.fi0) / this.alfa);
	  this.g = Math.pow((1 + this.e * Math.sin(this.fi0)) / (1 - this.e * Math.sin(this.fi0)), this.alfa * this.e / 2);
	  this.k = Math.tan(this.u0 / 2 + this.s45) / Math.pow(Math.tan(this.fi0 / 2 + this.s45), this.alfa) * this.g;
	  this.k1 = this.k0;
	  this.n0 = this.a * Math.sqrt(1 - this.e2) / (1 - this.e2 * Math.pow(Math.sin(this.fi0), 2));
	  this.s0 = 1.37008346281555;
	  this.n = Math.sin(this.s0);
	  this.ro0 = this.k1 * this.n0 / Math.tan(this.s0);
	  this.ad = this.s90 - this.uq;
	};

	/* ellipsoid */
	/* calculate xy from lat/lon */
	/* Constants, identical to inverse transform function */
	exports.forward = function(p) {
	  var gfi, u, deltav, s, d, eps, ro;
	  var lon = p.x;
	  var lat = p.y;
	  var delta_lon = adjust_lon(lon - this.long0);
	  /* Transformation */
	  gfi = Math.pow(((1 + this.e * Math.sin(lat)) / (1 - this.e * Math.sin(lat))), (this.alfa * this.e / 2));
	  u = 2 * (Math.atan(this.k * Math.pow(Math.tan(lat / 2 + this.s45), this.alfa) / gfi) - this.s45);
	  deltav = -delta_lon * this.alfa;
	  s = Math.asin(Math.cos(this.ad) * Math.sin(u) + Math.sin(this.ad) * Math.cos(u) * Math.cos(deltav));
	  d = Math.asin(Math.cos(u) * Math.sin(deltav) / Math.cos(s));
	  eps = this.n * d;
	  ro = this.ro0 * Math.pow(Math.tan(this.s0 / 2 + this.s45), this.n) / Math.pow(Math.tan(s / 2 + this.s45), this.n);
	  p.y = ro * Math.cos(eps) / 1;
	  p.x = ro * Math.sin(eps) / 1;

	  if (!this.czech) {
	    p.y *= -1;
	    p.x *= -1;
	  }
	  return (p);
	};

	/* calculate lat/lon from xy */
	exports.inverse = function(p) {
	  var u, deltav, s, d, eps, ro, fi1;
	  var ok;

	  /* Transformation */
	  /* revert y, x*/
	  var tmp = p.x;
	  p.x = p.y;
	  p.y = tmp;
	  if (!this.czech) {
	    p.y *= -1;
	    p.x *= -1;
	  }
	  ro = Math.sqrt(p.x * p.x + p.y * p.y);
	  eps = Math.atan2(p.y, p.x);
	  d = eps / Math.sin(this.s0);
	  s = 2 * (Math.atan(Math.pow(this.ro0 / ro, 1 / this.n) * Math.tan(this.s0 / 2 + this.s45)) - this.s45);
	  u = Math.asin(Math.cos(this.ad) * Math.sin(s) - Math.sin(this.ad) * Math.cos(s) * Math.cos(d));
	  deltav = Math.asin(Math.cos(s) * Math.sin(d) / Math.cos(u));
	  p.x = this.long0 - deltav / this.alfa;
	  fi1 = u;
	  ok = 0;
	  var iter = 0;
	  do {
	    p.y = 2 * (Math.atan(Math.pow(this.k, - 1 / this.alfa) * Math.pow(Math.tan(u / 2 + this.s45), 1 / this.alfa) * Math.pow((1 + this.e * Math.sin(fi1)) / (1 - this.e * Math.sin(fi1)), this.e / 2)) - this.s45);
	    if (Math.abs(fi1 - p.y) < 0.0000000001) {
	      ok = 1;
	    }
	    fi1 = p.y;
	    iter += 1;
	  } while (ok === 0 && iter < 15);
	  if (iter >= 15) {
	    return null;
	  }

	  return (p);
	};
	exports.names = ["Krovak", "krovak"];
	});

	var require$$14 = (krovak && typeof krovak === 'object' && 'default' in krovak ? krovak['default'] : krovak);

	var phi2z = createCommonjsModule(function (module) {
	var HALF_PI = Math.PI/2;
	module.exports = function(eccent, ts) {
	  var eccnth = 0.5 * eccent;
	  var con, dphi;
	  var phi = HALF_PI - 2 * Math.atan(ts);
	  for (var i = 0; i <= 15; i++) {
	    con = eccent * Math.sin(phi);
	    dphi = HALF_PI - 2 * Math.atan(ts * (Math.pow(((1 - con) / (1 + con)), eccnth))) - phi;
	    phi += dphi;
	    if (Math.abs(dphi) <= 0.0000000001) {
	      return phi;
	    }
	  }
	  //console.log("phi2z has NoConvergence");
	  return -9999;
	};
	});

	var require$$0$7 = (phi2z && typeof phi2z === 'object' && 'default' in phi2z ? phi2z['default'] : phi2z);

	var tsfnz = createCommonjsModule(function (module) {
	var HALF_PI = Math.PI/2;

	module.exports = function(eccent, phi, sinphi) {
	  var con = eccent * sinphi;
	  var com = 0.5 * eccent;
	  con = Math.pow(((1 - con) / (1 + con)), com);
	  return (Math.tan(0.5 * (HALF_PI - phi)) / con);
	};
	});

	var require$$1$5 = (tsfnz && typeof tsfnz === 'object' && 'default' in tsfnz ? tsfnz['default'] : tsfnz);

	var lcc = createCommonjsModule(function (module, exports) {
	var EPSLN = 1.0e-10;
	var msfnz = require$$3$2;
	var tsfnz = require$$1$5;
	var HALF_PI = Math.PI/2;
	var sign = require$$1;
	var adjust_lon = require$$2;
	var phi2z = require$$0$7;
	exports.init = function() {

	  // array of:  r_maj,r_min,lat1,lat2,c_lon,c_lat,false_east,false_north
	  //double c_lat;                   /* center latitude                      */
	  //double c_lon;                   /* center longitude                     */
	  //double lat1;                    /* first standard parallel              */
	  //double lat2;                    /* second standard parallel             */
	  //double r_maj;                   /* major axis                           */
	  //double r_min;                   /* minor axis                           */
	  //double false_east;              /* x offset in meters                   */
	  //double false_north;             /* y offset in meters                   */

	  if (!this.lat2) {
	    this.lat2 = this.lat1;
	  } //if lat2 is not defined
	  if (!this.k0) {
	    this.k0 = 1;
	  }
	  this.x0 = this.x0 || 0;
	  this.y0 = this.y0 || 0;
	  // Standard Parallels cannot be equal and on opposite sides of the equator
	  if (Math.abs(this.lat1 + this.lat2) < EPSLN) {
	    return;
	  }

	  var temp = this.b / this.a;
	  this.e = Math.sqrt(1 - temp * temp);

	  var sin1 = Math.sin(this.lat1);
	  var cos1 = Math.cos(this.lat1);
	  var ms1 = msfnz(this.e, sin1, cos1);
	  var ts1 = tsfnz(this.e, this.lat1, sin1);

	  var sin2 = Math.sin(this.lat2);
	  var cos2 = Math.cos(this.lat2);
	  var ms2 = msfnz(this.e, sin2, cos2);
	  var ts2 = tsfnz(this.e, this.lat2, sin2);

	  var ts0 = tsfnz(this.e, this.lat0, Math.sin(this.lat0));

	  if (Math.abs(this.lat1 - this.lat2) > EPSLN) {
	    this.ns = Math.log(ms1 / ms2) / Math.log(ts1 / ts2);
	  }
	  else {
	    this.ns = sin1;
	  }
	  if (isNaN(this.ns)) {
	    this.ns = sin1;
	  }
	  this.f0 = ms1 / (this.ns * Math.pow(ts1, this.ns));
	  this.rh = this.a * this.f0 * Math.pow(ts0, this.ns);
	  if (!this.title) {
	    this.title = "Lambert Conformal Conic";
	  }
	};


	// Lambert Conformal conic forward equations--mapping lat,long to x,y
	// -----------------------------------------------------------------
	exports.forward = function(p) {

	  var lon = p.x;
	  var lat = p.y;

	  // singular cases :
	  if (Math.abs(2 * Math.abs(lat) - Math.PI) <= EPSLN) {
	    lat = sign(lat) * (HALF_PI - 2 * EPSLN);
	  }

	  var con = Math.abs(Math.abs(lat) - HALF_PI);
	  var ts, rh1;
	  if (con > EPSLN) {
	    ts = tsfnz(this.e, lat, Math.sin(lat));
	    rh1 = this.a * this.f0 * Math.pow(ts, this.ns);
	  }
	  else {
	    con = lat * this.ns;
	    if (con <= 0) {
	      return null;
	    }
	    rh1 = 0;
	  }
	  var theta = this.ns * adjust_lon(lon - this.long0);
	  p.x = this.k0 * (rh1 * Math.sin(theta)) + this.x0;
	  p.y = this.k0 * (this.rh - rh1 * Math.cos(theta)) + this.y0;

	  return p;
	};

	// Lambert Conformal Conic inverse equations--mapping x,y to lat/long
	// -----------------------------------------------------------------
	exports.inverse = function(p) {

	  var rh1, con, ts;
	  var lat, lon;
	  var x = (p.x - this.x0) / this.k0;
	  var y = (this.rh - (p.y - this.y0) / this.k0);
	  if (this.ns > 0) {
	    rh1 = Math.sqrt(x * x + y * y);
	    con = 1;
	  }
	  else {
	    rh1 = -Math.sqrt(x * x + y * y);
	    con = -1;
	  }
	  var theta = 0;
	  if (rh1 !== 0) {
	    theta = Math.atan2((con * x), (con * y));
	  }
	  if ((rh1 !== 0) || (this.ns > 0)) {
	    con = 1 / this.ns;
	    ts = Math.pow((rh1 / (this.a * this.f0)), con);
	    lat = phi2z(this.e, ts);
	    if (lat === -9999) {
	      return null;
	    }
	  }
	  else {
	    lat = -HALF_PI;
	  }
	  lon = adjust_lon(theta / this.ns + this.long0);

	  p.x = lon;
	  p.y = lat;
	  return p;
	};

	exports.names = ["Lambert Tangential Conformal Conic Projection", "Lambert_Conformal_Conic", "Lambert_Conformal_Conic_2SP", "lcc"];
	});

	var require$$15 = (lcc && typeof lcc === 'object' && 'default' in lcc ? lcc['default'] : lcc);

	var omerc = createCommonjsModule(function (module, exports) {
	var tsfnz = require$$1$5;
	var adjust_lon = require$$2;
	var phi2z = require$$0$7;
	var HALF_PI = Math.PI/2;
	var FORTPI = Math.PI/4;
	var EPSLN = 1.0e-10;

	/* Initialize the Oblique Mercator  projection
	    ------------------------------------------*/
	exports.init = function() {
	  this.no_off = this.no_off || false;
	  this.no_rot = this.no_rot || false;

	  if (isNaN(this.k0)) {
	    this.k0 = 1;
	  }
	  var sinlat = Math.sin(this.lat0);
	  var coslat = Math.cos(this.lat0);
	  var con = this.e * sinlat;

	  this.bl = Math.sqrt(1 + this.es / (1 - this.es) * Math.pow(coslat, 4));
	  this.al = this.a * this.bl * this.k0 * Math.sqrt(1 - this.es) / (1 - con * con);
	  var t0 = tsfnz(this.e, this.lat0, sinlat);
	  var dl = this.bl / coslat * Math.sqrt((1 - this.es) / (1 - con * con));
	  if (dl * dl < 1) {
	    dl = 1;
	  }
	  var fl;
	  var gl;
	  if (!isNaN(this.longc)) {
	    //Central point and azimuth method

	    if (this.lat0 >= 0) {
	      fl = dl + Math.sqrt(dl * dl - 1);
	    }
	    else {
	      fl = dl - Math.sqrt(dl * dl - 1);
	    }
	    this.el = fl * Math.pow(t0, this.bl);
	    gl = 0.5 * (fl - 1 / fl);
	    this.gamma0 = Math.asin(Math.sin(this.alpha) / dl);
	    this.long0 = this.longc - Math.asin(gl * Math.tan(this.gamma0)) / this.bl;

	  }
	  else {
	    //2 points method
	    var t1 = tsfnz(this.e, this.lat1, Math.sin(this.lat1));
	    var t2 = tsfnz(this.e, this.lat2, Math.sin(this.lat2));
	    if (this.lat0 >= 0) {
	      this.el = (dl + Math.sqrt(dl * dl - 1)) * Math.pow(t0, this.bl);
	    }
	    else {
	      this.el = (dl - Math.sqrt(dl * dl - 1)) * Math.pow(t0, this.bl);
	    }
	    var hl = Math.pow(t1, this.bl);
	    var ll = Math.pow(t2, this.bl);
	    fl = this.el / hl;
	    gl = 0.5 * (fl - 1 / fl);
	    var jl = (this.el * this.el - ll * hl) / (this.el * this.el + ll * hl);
	    var pl = (ll - hl) / (ll + hl);
	    var dlon12 = adjust_lon(this.long1 - this.long2);
	    this.long0 = 0.5 * (this.long1 + this.long2) - Math.atan(jl * Math.tan(0.5 * this.bl * (dlon12)) / pl) / this.bl;
	    this.long0 = adjust_lon(this.long0);
	    var dlon10 = adjust_lon(this.long1 - this.long0);
	    this.gamma0 = Math.atan(Math.sin(this.bl * (dlon10)) / gl);
	    this.alpha = Math.asin(dl * Math.sin(this.gamma0));
	  }

	  if (this.no_off) {
	    this.uc = 0;
	  }
	  else {
	    if (this.lat0 >= 0) {
	      this.uc = this.al / this.bl * Math.atan2(Math.sqrt(dl * dl - 1), Math.cos(this.alpha));
	    }
	    else {
	      this.uc = -1 * this.al / this.bl * Math.atan2(Math.sqrt(dl * dl - 1), Math.cos(this.alpha));
	    }
	  }

	};


	/* Oblique Mercator forward equations--mapping lat,long to x,y
	    ----------------------------------------------------------*/
	exports.forward = function(p) {
	  var lon = p.x;
	  var lat = p.y;
	  var dlon = adjust_lon(lon - this.long0);
	  var us, vs;
	  var con;
	  if (Math.abs(Math.abs(lat) - HALF_PI) <= EPSLN) {
	    if (lat > 0) {
	      con = -1;
	    }
	    else {
	      con = 1;
	    }
	    vs = this.al / this.bl * Math.log(Math.tan(FORTPI + con * this.gamma0 * 0.5));
	    us = -1 * con * HALF_PI * this.al / this.bl;
	  }
	  else {
	    var t = tsfnz(this.e, lat, Math.sin(lat));
	    var ql = this.el / Math.pow(t, this.bl);
	    var sl = 0.5 * (ql - 1 / ql);
	    var tl = 0.5 * (ql + 1 / ql);
	    var vl = Math.sin(this.bl * (dlon));
	    var ul = (sl * Math.sin(this.gamma0) - vl * Math.cos(this.gamma0)) / tl;
	    if (Math.abs(Math.abs(ul) - 1) <= EPSLN) {
	      vs = Number.POSITIVE_INFINITY;
	    }
	    else {
	      vs = 0.5 * this.al * Math.log((1 - ul) / (1 + ul)) / this.bl;
	    }
	    if (Math.abs(Math.cos(this.bl * (dlon))) <= EPSLN) {
	      us = this.al * this.bl * (dlon);
	    }
	    else {
	      us = this.al * Math.atan2(sl * Math.cos(this.gamma0) + vl * Math.sin(this.gamma0), Math.cos(this.bl * dlon)) / this.bl;
	    }
	  }

	  if (this.no_rot) {
	    p.x = this.x0 + us;
	    p.y = this.y0 + vs;
	  }
	  else {

	    us -= this.uc;
	    p.x = this.x0 + vs * Math.cos(this.alpha) + us * Math.sin(this.alpha);
	    p.y = this.y0 + us * Math.cos(this.alpha) - vs * Math.sin(this.alpha);
	  }
	  return p;
	};

	exports.inverse = function(p) {
	  var us, vs;
	  if (this.no_rot) {
	    vs = p.y - this.y0;
	    us = p.x - this.x0;
	  }
	  else {
	    vs = (p.x - this.x0) * Math.cos(this.alpha) - (p.y - this.y0) * Math.sin(this.alpha);
	    us = (p.y - this.y0) * Math.cos(this.alpha) + (p.x - this.x0) * Math.sin(this.alpha);
	    us += this.uc;
	  }
	  var qp = Math.exp(-1 * this.bl * vs / this.al);
	  var sp = 0.5 * (qp - 1 / qp);
	  var tp = 0.5 * (qp + 1 / qp);
	  var vp = Math.sin(this.bl * us / this.al);
	  var up = (vp * Math.cos(this.gamma0) + sp * Math.sin(this.gamma0)) / tp;
	  var ts = Math.pow(this.el / Math.sqrt((1 + up) / (1 - up)), 1 / this.bl);
	  if (Math.abs(up - 1) < EPSLN) {
	    p.x = this.long0;
	    p.y = HALF_PI;
	  }
	  else if (Math.abs(up + 1) < EPSLN) {
	    p.x = this.long0;
	    p.y = -1 * HALF_PI;
	  }
	  else {
	    p.y = phi2z(this.e, ts);
	    p.x = adjust_lon(this.long0 - Math.atan2(sp * Math.cos(this.gamma0) - vp * Math.sin(this.gamma0), Math.cos(this.bl * us / this.al)) / this.bl);
	  }
	  return p;
	};

	exports.names = ["Hotine_Oblique_Mercator", "Hotine Oblique Mercator", "Hotine_Oblique_Mercator_Azimuth_Natural_Origin", "Hotine_Oblique_Mercator_Azimuth_Center", "omerc"];
	});

	var require$$16 = (omerc && typeof omerc === 'object' && 'default' in omerc ? omerc['default'] : omerc);

	var somerc = createCommonjsModule(function (module, exports) {
	/*
	  references:
	    Formules et constantes pour le Calcul pour la
	    projection cylindrique conforme à axe oblique et pour la transformation entre
	    des systèmes de référence.
	    http://www.swisstopo.admin.ch/internet/swisstopo/fr/home/topics/survey/sys/refsys/switzerland.parsysrelated1.31216.downloadList.77004.DownloadFile.tmp/swissprojectionfr.pdf
	  */
	exports.init = function() {
	  var phy0 = this.lat0;
	  this.lambda0 = this.long0;
	  var sinPhy0 = Math.sin(phy0);
	  var semiMajorAxis = this.a;
	  var invF = this.rf;
	  var flattening = 1 / invF;
	  var e2 = 2 * flattening - Math.pow(flattening, 2);
	  var e = this.e = Math.sqrt(e2);
	  this.R = this.k0 * semiMajorAxis * Math.sqrt(1 - e2) / (1 - e2 * Math.pow(sinPhy0, 2));
	  this.alpha = Math.sqrt(1 + e2 / (1 - e2) * Math.pow(Math.cos(phy0), 4));
	  this.b0 = Math.asin(sinPhy0 / this.alpha);
	  var k1 = Math.log(Math.tan(Math.PI / 4 + this.b0 / 2));
	  var k2 = Math.log(Math.tan(Math.PI / 4 + phy0 / 2));
	  var k3 = Math.log((1 + e * sinPhy0) / (1 - e * sinPhy0));
	  this.K = k1 - this.alpha * k2 + this.alpha * e / 2 * k3;
	};


	exports.forward = function(p) {
	  var Sa1 = Math.log(Math.tan(Math.PI / 4 - p.y / 2));
	  var Sa2 = this.e / 2 * Math.log((1 + this.e * Math.sin(p.y)) / (1 - this.e * Math.sin(p.y)));
	  var S = -this.alpha * (Sa1 + Sa2) + this.K;

	  // spheric latitude
	  var b = 2 * (Math.atan(Math.exp(S)) - Math.PI / 4);

	  // spheric longitude
	  var I = this.alpha * (p.x - this.lambda0);

	  // psoeudo equatorial rotation
	  var rotI = Math.atan(Math.sin(I) / (Math.sin(this.b0) * Math.tan(b) + Math.cos(this.b0) * Math.cos(I)));

	  var rotB = Math.asin(Math.cos(this.b0) * Math.sin(b) - Math.sin(this.b0) * Math.cos(b) * Math.cos(I));

	  p.y = this.R / 2 * Math.log((1 + Math.sin(rotB)) / (1 - Math.sin(rotB))) + this.y0;
	  p.x = this.R * rotI + this.x0;
	  return p;
	};

	exports.inverse = function(p) {
	  var Y = p.x - this.x0;
	  var X = p.y - this.y0;

	  var rotI = Y / this.R;
	  var rotB = 2 * (Math.atan(Math.exp(X / this.R)) - Math.PI / 4);

	  var b = Math.asin(Math.cos(this.b0) * Math.sin(rotB) + Math.sin(this.b0) * Math.cos(rotB) * Math.cos(rotI));
	  var I = Math.atan(Math.sin(rotI) / (Math.cos(this.b0) * Math.cos(rotI) - Math.sin(this.b0) * Math.tan(rotB)));

	  var lambda = this.lambda0 + I / this.alpha;

	  var S = 0;
	  var phy = b;
	  var prevPhy = -1000;
	  var iteration = 0;
	  while (Math.abs(phy - prevPhy) > 0.0000001) {
	    if (++iteration > 20) {
	      //...reportError("omercFwdInfinity");
	      return;
	    }
	    //S = Math.log(Math.tan(Math.PI / 4 + phy / 2));
	    S = 1 / this.alpha * (Math.log(Math.tan(Math.PI / 4 + b / 2)) - this.K) + this.e * Math.log(Math.tan(Math.PI / 4 + Math.asin(this.e * Math.sin(phy)) / 2));
	    prevPhy = phy;
	    phy = 2 * Math.atan(Math.exp(S)) - Math.PI / 2;
	  }

	  p.x = lambda;
	  p.y = phy;
	  return p;
	};

	exports.names = ["somerc"];
	});

	var require$$17 = (somerc && typeof somerc === 'object' && 'default' in somerc ? somerc['default'] : somerc);

	var stere = createCommonjsModule(function (module, exports) {
	var HALF_PI = Math.PI/2;
	var EPSLN = 1.0e-10;
	var sign = require$$1;
	var msfnz = require$$3$2;
	var tsfnz = require$$1$5;
	var phi2z = require$$0$7;
	var adjust_lon = require$$2;
	exports.ssfn_ = function(phit, sinphi, eccen) {
	  sinphi *= eccen;
	  return (Math.tan(0.5 * (HALF_PI + phit)) * Math.pow((1 - sinphi) / (1 + sinphi), 0.5 * eccen));
	};

	exports.init = function() {
	  this.coslat0 = Math.cos(this.lat0);
	  this.sinlat0 = Math.sin(this.lat0);
	  if (this.sphere) {
	    if (this.k0 === 1 && !isNaN(this.lat_ts) && Math.abs(this.coslat0) <= EPSLN) {
	      this.k0 = 0.5 * (1 + sign(this.lat0) * Math.sin(this.lat_ts));
	    }
	  }
	  else {
	    if (Math.abs(this.coslat0) <= EPSLN) {
	      if (this.lat0 > 0) {
	        //North pole
	        //trace('stere:north pole');
	        this.con = 1;
	      }
	      else {
	        //South pole
	        //trace('stere:south pole');
	        this.con = -1;
	      }
	    }
	    this.cons = Math.sqrt(Math.pow(1 + this.e, 1 + this.e) * Math.pow(1 - this.e, 1 - this.e));
	    if (this.k0 === 1 && !isNaN(this.lat_ts) && Math.abs(this.coslat0) <= EPSLN) {
	      this.k0 = 0.5 * this.cons * msfnz(this.e, Math.sin(this.lat_ts), Math.cos(this.lat_ts)) / tsfnz(this.e, this.con * this.lat_ts, this.con * Math.sin(this.lat_ts));
	    }
	    this.ms1 = msfnz(this.e, this.sinlat0, this.coslat0);
	    this.X0 = 2 * Math.atan(this.ssfn_(this.lat0, this.sinlat0, this.e)) - HALF_PI;
	    this.cosX0 = Math.cos(this.X0);
	    this.sinX0 = Math.sin(this.X0);
	  }
	};

	// Stereographic forward equations--mapping lat,long to x,y
	exports.forward = function(p) {
	  var lon = p.x;
	  var lat = p.y;
	  var sinlat = Math.sin(lat);
	  var coslat = Math.cos(lat);
	  var A, X, sinX, cosX, ts, rh;
	  var dlon = adjust_lon(lon - this.long0);

	  if (Math.abs(Math.abs(lon - this.long0) - Math.PI) <= EPSLN && Math.abs(lat + this.lat0) <= EPSLN) {
	    //case of the origine point
	    //trace('stere:this is the origin point');
	    p.x = NaN;
	    p.y = NaN;
	    return p;
	  }
	  if (this.sphere) {
	    //trace('stere:sphere case');
	    A = 2 * this.k0 / (1 + this.sinlat0 * sinlat + this.coslat0 * coslat * Math.cos(dlon));
	    p.x = this.a * A * coslat * Math.sin(dlon) + this.x0;
	    p.y = this.a * A * (this.coslat0 * sinlat - this.sinlat0 * coslat * Math.cos(dlon)) + this.y0;
	    return p;
	  }
	  else {
	    X = 2 * Math.atan(this.ssfn_(lat, sinlat, this.e)) - HALF_PI;
	    cosX = Math.cos(X);
	    sinX = Math.sin(X);
	    if (Math.abs(this.coslat0) <= EPSLN) {
	      ts = tsfnz(this.e, lat * this.con, this.con * sinlat);
	      rh = 2 * this.a * this.k0 * ts / this.cons;
	      p.x = this.x0 + rh * Math.sin(lon - this.long0);
	      p.y = this.y0 - this.con * rh * Math.cos(lon - this.long0);
	      //trace(p.toString());
	      return p;
	    }
	    else if (Math.abs(this.sinlat0) < EPSLN) {
	      //Eq
	      //trace('stere:equateur');
	      A = 2 * this.a * this.k0 / (1 + cosX * Math.cos(dlon));
	      p.y = A * sinX;
	    }
	    else {
	      //other case
	      //trace('stere:normal case');
	      A = 2 * this.a * this.k0 * this.ms1 / (this.cosX0 * (1 + this.sinX0 * sinX + this.cosX0 * cosX * Math.cos(dlon)));
	      p.y = A * (this.cosX0 * sinX - this.sinX0 * cosX * Math.cos(dlon)) + this.y0;
	    }
	    p.x = A * cosX * Math.sin(dlon) + this.x0;
	  }
	  //trace(p.toString());
	  return p;
	};


	//* Stereographic inverse equations--mapping x,y to lat/long
	exports.inverse = function(p) {
	  p.x -= this.x0;
	  p.y -= this.y0;
	  var lon, lat, ts, ce, Chi;
	  var rh = Math.sqrt(p.x * p.x + p.y * p.y);
	  if (this.sphere) {
	    var c = 2 * Math.atan(rh / (0.5 * this.a * this.k0));
	    lon = this.long0;
	    lat = this.lat0;
	    if (rh <= EPSLN) {
	      p.x = lon;
	      p.y = lat;
	      return p;
	    }
	    lat = Math.asin(Math.cos(c) * this.sinlat0 + p.y * Math.sin(c) * this.coslat0 / rh);
	    if (Math.abs(this.coslat0) < EPSLN) {
	      if (this.lat0 > 0) {
	        lon = adjust_lon(this.long0 + Math.atan2(p.x, - 1 * p.y));
	      }
	      else {
	        lon = adjust_lon(this.long0 + Math.atan2(p.x, p.y));
	      }
	    }
	    else {
	      lon = adjust_lon(this.long0 + Math.atan2(p.x * Math.sin(c), rh * this.coslat0 * Math.cos(c) - p.y * this.sinlat0 * Math.sin(c)));
	    }
	    p.x = lon;
	    p.y = lat;
	    return p;
	  }
	  else {
	    if (Math.abs(this.coslat0) <= EPSLN) {
	      if (rh <= EPSLN) {
	        lat = this.lat0;
	        lon = this.long0;
	        p.x = lon;
	        p.y = lat;
	        //trace(p.toString());
	        return p;
	      }
	      p.x *= this.con;
	      p.y *= this.con;
	      ts = rh * this.cons / (2 * this.a * this.k0);
	      lat = this.con * phi2z(this.e, ts);
	      lon = this.con * adjust_lon(this.con * this.long0 + Math.atan2(p.x, - 1 * p.y));
	    }
	    else {
	      ce = 2 * Math.atan(rh * this.cosX0 / (2 * this.a * this.k0 * this.ms1));
	      lon = this.long0;
	      if (rh <= EPSLN) {
	        Chi = this.X0;
	      }
	      else {
	        Chi = Math.asin(Math.cos(ce) * this.sinX0 + p.y * Math.sin(ce) * this.cosX0 / rh);
	        lon = adjust_lon(this.long0 + Math.atan2(p.x * Math.sin(ce), rh * this.cosX0 * Math.cos(ce) - p.y * this.sinX0 * Math.sin(ce)));
	      }
	      lat = -1 * phi2z(this.e, Math.tan(0.5 * (HALF_PI + Chi)));
	    }
	  }
	  p.x = lon;
	  p.y = lat;

	  //trace(p.toString());
	  return p;

	};
	exports.names = ["stere", "Stereographic_South_Pole", "Polar Stereographic (variant B)"];
	});

	var require$$18 = (stere && typeof stere === 'object' && 'default' in stere ? stere['default'] : stere);

	var srat = createCommonjsModule(function (module) {
	module.exports = function(esinp, exp) {
	  return (Math.pow((1 - esinp) / (1 + esinp), exp));
	};
	});

	var require$$0$8 = (srat && typeof srat === 'object' && 'default' in srat ? srat['default'] : srat);

	var gauss = createCommonjsModule(function (module, exports) {
	var FORTPI = Math.PI/4;
	var srat = require$$0$8;
	var HALF_PI = Math.PI/2;
	var MAX_ITER = 20;
	exports.init = function() {
	  var sphi = Math.sin(this.lat0);
	  var cphi = Math.cos(this.lat0);
	  cphi *= cphi;
	  this.rc = Math.sqrt(1 - this.es) / (1 - this.es * sphi * sphi);
	  this.C = Math.sqrt(1 + this.es * cphi * cphi / (1 - this.es));
	  this.phic0 = Math.asin(sphi / this.C);
	  this.ratexp = 0.5 * this.C * this.e;
	  this.K = Math.tan(0.5 * this.phic0 + FORTPI) / (Math.pow(Math.tan(0.5 * this.lat0 + FORTPI), this.C) * srat(this.e * sphi, this.ratexp));
	};

	exports.forward = function(p) {
	  var lon = p.x;
	  var lat = p.y;

	  p.y = 2 * Math.atan(this.K * Math.pow(Math.tan(0.5 * lat + FORTPI), this.C) * srat(this.e * Math.sin(lat), this.ratexp)) - HALF_PI;
	  p.x = this.C * lon;
	  return p;
	};

	exports.inverse = function(p) {
	  var DEL_TOL = 1e-14;
	  var lon = p.x / this.C;
	  var lat = p.y;
	  var num = Math.pow(Math.tan(0.5 * lat + FORTPI) / this.K, 1 / this.C);
	  for (var i = MAX_ITER; i > 0; --i) {
	    lat = 2 * Math.atan(num * srat(this.e * Math.sin(p.y), - 0.5 * this.e)) - HALF_PI;
	    if (Math.abs(lat - p.y) < DEL_TOL) {
	      break;
	    }
	    p.y = lat;
	  }
	  /* convergence failed */
	  if (!i) {
	    return null;
	  }
	  p.x = lon;
	  p.y = lat;
	  return p;
	};
	exports.names = ["gauss"];
	});

	var require$$1$6 = (gauss && typeof gauss === 'object' && 'default' in gauss ? gauss['default'] : gauss);

	var sterea = createCommonjsModule(function (module, exports) {
	var gauss = require$$1$6;
	var adjust_lon = require$$2;
	exports.init = function() {
	  gauss.init.apply(this);
	  if (!this.rc) {
	    return;
	  }
	  this.sinc0 = Math.sin(this.phic0);
	  this.cosc0 = Math.cos(this.phic0);
	  this.R2 = 2 * this.rc;
	  if (!this.title) {
	    this.title = "Oblique Stereographic Alternative";
	  }
	};

	exports.forward = function(p) {
	  var sinc, cosc, cosl, k;
	  p.x = adjust_lon(p.x - this.long0);
	  gauss.forward.apply(this, [p]);
	  sinc = Math.sin(p.y);
	  cosc = Math.cos(p.y);
	  cosl = Math.cos(p.x);
	  k = this.k0 * this.R2 / (1 + this.sinc0 * sinc + this.cosc0 * cosc * cosl);
	  p.x = k * cosc * Math.sin(p.x);
	  p.y = k * (this.cosc0 * sinc - this.sinc0 * cosc * cosl);
	  p.x = this.a * p.x + this.x0;
	  p.y = this.a * p.y + this.y0;
	  return p;
	};

	exports.inverse = function(p) {
	  var sinc, cosc, lon, lat, rho;
	  p.x = (p.x - this.x0) / this.a;
	  p.y = (p.y - this.y0) / this.a;

	  p.x /= this.k0;
	  p.y /= this.k0;
	  if ((rho = Math.sqrt(p.x * p.x + p.y * p.y))) {
	    var c = 2 * Math.atan2(rho, this.R2);
	    sinc = Math.sin(c);
	    cosc = Math.cos(c);
	    lat = Math.asin(cosc * this.sinc0 + p.y * sinc * this.cosc0 / rho);
	    lon = Math.atan2(p.x * sinc, rho * this.cosc0 * cosc - p.y * this.sinc0 * sinc);
	  }
	  else {
	    lat = this.phic0;
	    lon = 0;
	  }

	  p.x = lon;
	  p.y = lat;
	  gauss.inverse.apply(this, [p]);
	  p.x = adjust_lon(p.x + this.long0);
	  return p;
	};

	exports.names = ["Stereographic_North_Pole", "Oblique_Stereographic", "Polar_Stereographic", "sterea","Oblique Stereographic Alternative"];
	});

	var require$$19 = (sterea && typeof sterea === 'object' && 'default' in sterea ? sterea['default'] : sterea);

	var tmerc = createCommonjsModule(function (module, exports) {
	var e0fn = require$$7;
	var e1fn = require$$6;
	var e2fn = require$$5;
	var e3fn = require$$4;
	var mlfn = require$$3$1;
	var adjust_lon = require$$2;
	var HALF_PI = Math.PI/2;
	var EPSLN = 1.0e-10;
	var sign = require$$1;
	var asinz = require$$0$4;

	exports.init = function() {
	  this.e0 = e0fn(this.es);
	  this.e1 = e1fn(this.es);
	  this.e2 = e2fn(this.es);
	  this.e3 = e3fn(this.es);
	  this.ml0 = this.a * mlfn(this.e0, this.e1, this.e2, this.e3, this.lat0);
	};

	/**
	    Transverse Mercator Forward  - long/lat to x/y
	    long/lat in radians
	  */
	exports.forward = function(p) {
	  var lon = p.x;
	  var lat = p.y;

	  var delta_lon = adjust_lon(lon - this.long0);
	  var con;
	  var x, y;
	  var sin_phi = Math.sin(lat);
	  var cos_phi = Math.cos(lat);

	  if (this.sphere) {
	    var b = cos_phi * Math.sin(delta_lon);
	    if ((Math.abs(Math.abs(b) - 1)) < 0.0000000001) {
	      return (93);
	    }
	    else {
	      x = 0.5 * this.a * this.k0 * Math.log((1 + b) / (1 - b));
	      con = Math.acos(cos_phi * Math.cos(delta_lon) / Math.sqrt(1 - b * b));
	      if (lat < 0) {
	        con = -con;
	      }
	      y = this.a * this.k0 * (con - this.lat0);
	    }
	  }
	  else {
	    var al = cos_phi * delta_lon;
	    var als = Math.pow(al, 2);
	    var c = this.ep2 * Math.pow(cos_phi, 2);
	    var tq = Math.tan(lat);
	    var t = Math.pow(tq, 2);
	    con = 1 - this.es * Math.pow(sin_phi, 2);
	    var n = this.a / Math.sqrt(con);
	    var ml = this.a * mlfn(this.e0, this.e1, this.e2, this.e3, lat);

	    x = this.k0 * n * al * (1 + als / 6 * (1 - t + c + als / 20 * (5 - 18 * t + Math.pow(t, 2) + 72 * c - 58 * this.ep2))) + this.x0;
	    y = this.k0 * (ml - this.ml0 + n * tq * (als * (0.5 + als / 24 * (5 - t + 9 * c + 4 * Math.pow(c, 2) + als / 30 * (61 - 58 * t + Math.pow(t, 2) + 600 * c - 330 * this.ep2))))) + this.y0;

	  }
	  p.x = x;
	  p.y = y;
	  return p;
	};

	/**
	    Transverse Mercator Inverse  -  x/y to long/lat
	  */
	exports.inverse = function(p) {
	  var con, phi;
	  var delta_phi;
	  var i;
	  var max_iter = 6;
	  var lat, lon;

	  if (this.sphere) {
	    var f = Math.exp(p.x / (this.a * this.k0));
	    var g = 0.5 * (f - 1 / f);
	    var temp = this.lat0 + p.y / (this.a * this.k0);
	    var h = Math.cos(temp);
	    con = Math.sqrt((1 - h * h) / (1 + g * g));
	    lat = asinz(con);
	    if (temp < 0) {
	      lat = -lat;
	    }
	    if ((g === 0) && (h === 0)) {
	      lon = this.long0;
	    }
	    else {
	      lon = adjust_lon(Math.atan2(g, h) + this.long0);
	    }
	  }
	  else { // ellipsoidal form
	    var x = p.x - this.x0;
	    var y = p.y - this.y0;

	    con = (this.ml0 + y / this.k0) / this.a;
	    phi = con;
	    for (i = 0; true; i++) {
	      delta_phi = ((con + this.e1 * Math.sin(2 * phi) - this.e2 * Math.sin(4 * phi) + this.e3 * Math.sin(6 * phi)) / this.e0) - phi;
	      phi += delta_phi;
	      if (Math.abs(delta_phi) <= EPSLN) {
	        break;
	      }
	      if (i >= max_iter) {
	        return (95);
	      }
	    } // for()
	    if (Math.abs(phi) < HALF_PI) {
	      var sin_phi = Math.sin(phi);
	      var cos_phi = Math.cos(phi);
	      var tan_phi = Math.tan(phi);
	      var c = this.ep2 * Math.pow(cos_phi, 2);
	      var cs = Math.pow(c, 2);
	      var t = Math.pow(tan_phi, 2);
	      var ts = Math.pow(t, 2);
	      con = 1 - this.es * Math.pow(sin_phi, 2);
	      var n = this.a / Math.sqrt(con);
	      var r = n * (1 - this.es) / con;
	      var d = x / (n * this.k0);
	      var ds = Math.pow(d, 2);
	      lat = phi - (n * tan_phi * ds / r) * (0.5 - ds / 24 * (5 + 3 * t + 10 * c - 4 * cs - 9 * this.ep2 - ds / 30 * (61 + 90 * t + 298 * c + 45 * ts - 252 * this.ep2 - 3 * cs)));
	      lon = adjust_lon(this.long0 + (d * (1 - ds / 6 * (1 + 2 * t + c - ds / 20 * (5 - 2 * c + 28 * t - 3 * cs + 8 * this.ep2 + 24 * ts))) / cos_phi));
	    }
	    else {
	      lat = HALF_PI * sign(y);
	      lon = this.long0;
	    }
	  }
	  p.x = lon;
	  p.y = lat;
	  return p;
	};
	exports.names = ["Transverse_Mercator", "Transverse Mercator", "tmerc"];
	});

	var require$$0$9 = (tmerc && typeof tmerc === 'object' && 'default' in tmerc ? tmerc['default'] : tmerc);

	var utm = createCommonjsModule(function (module, exports) {
	var D2R = 0.01745329251994329577;
	var tmerc = require$$0$9;
	exports.dependsOn = 'tmerc';
	exports.init = function() {
	  if (!this.zone) {
	    return;
	  }
	  this.lat0 = 0;
	  this.long0 = ((6 * Math.abs(this.zone)) - 183) * D2R;
	  this.x0 = 500000;
	  this.y0 = this.utmSouth ? 10000000 : 0;
	  this.k0 = 0.9996;

	  tmerc.init.apply(this);
	  this.forward = tmerc.forward;
	  this.inverse = tmerc.inverse;
	};
	exports.names = ["Universal Transverse Mercator System", "utm"];
	});

	var require$$20 = (utm && typeof utm === 'object' && 'default' in utm ? utm['default'] : utm);

	var includedProjections = createCommonjsModule(function (module) {
	var projs = [
	  require$$0$9,
	  require$$20,
	  require$$19,
	  require$$18,
	  require$$17,
	  require$$16,
	  require$$15,
	  require$$14,
	  require$$13,
	  require$$12,
	  require$$11,
	  require$$10,
	  require$$9,
	  require$$8,
	  require$$7$1,
	  require$$6$1,
	  require$$5$1,
	  require$$4$1,
	  require$$3$3,
	  require$$2$1,
	  require$$1$1,
	  require$$0$2
	];
	module.exports = function(proj4){
	  projs.forEach(function(proj){
	    proj4.Proj.projections.add(proj);
	  });
	};
	});

	var require$$0$1 = (includedProjections && typeof includedProjections === 'object' && 'default' in includedProjections ? includedProjections['default'] : includedProjections);

	var name = "proj4";
	var version = "2.3.14";
	var description = "Proj4js is a JavaScript library to transform point coordinates from one coordinate system to another, including datum transformations.";
	var main = "lib/index.js";
	var directories = {"test":"test","doc":"docs"};
	var scripts = {"test":"./node_modules/istanbul/lib/cli.js test ./node_modules/mocha/bin/_mocha test/test.js"};
	var repository = {"type":"git","url":"git://github.com/proj4js/proj4js.git"};
	var author = "";
	var license = "MIT";
	var jam = {"main":"dist/proj4.js","include":["dist/proj4.js","README.md","AUTHORS","LICENSE.md"]};
	var devDependencies = {"grunt-cli":"~0.1.13","grunt":"~0.4.2","grunt-contrib-connect":"~0.6.0","grunt-contrib-jshint":"~0.8.0","chai":"~1.8.1","mocha":"~1.17.1","grunt-mocha-phantomjs":"~0.4.0","browserify":"~12.0.1","grunt-browserify":"~4.0.1","grunt-contrib-uglify":"~0.11.1","curl":"git://github.com/cujojs/curl.git","istanbul":"~0.2.4","tin":"~0.4.0"};
	var dependencies = {"mgrs":"~0.0.2"};
	var contributors = [{"name":"Mike Adair","email":"madair@dmsolutions.ca"},{"name":"Richard Greenwood","email":"rich@greenwoodmap.com"},{"name":"Calvin Metcalf","email":"calvin.metcalf@gmail.com"},{"name":"Richard Marsden","url":"http://www.winwaed.com"},{"name":"T. Mittan"},{"name":"D. Steinwand"},{"name":"S. Nelson"}];
	var gitHead = "7619c8a63df1eae5bad0b9ad31ca1d87b0549243";
	var bugs = {"url":"https://github.com/proj4js/proj4js/issues"};
	var homepage = "https://github.com/proj4js/proj4js#readme";
	var _id = "proj4@2.3.14";
	var _shasum = "928906144388980c914c5a357fc493aba59a747a";
	var _from = "proj4@>=2.3.14 <3.0.0";
	var _npmVersion = "2.14.12";
	var _nodeVersion = "4.2.6";
	var _npmUser = {"name":"ahocevar","email":"andreas.hocevar@gmail.com"};
	var dist = {"shasum":"928906144388980c914c5a357fc493aba59a747a","tarball":"https://registry.npmjs.org/proj4/-/proj4-2.3.14.tgz"};
	var maintainers = [{"name":"cwmma","email":"calvin.metcalf@gmail.com"},{"name":"ahocevar","email":"andreas.hocevar@gmail.com"}];
	var _npmOperationalInternal = {"host":"packages-13-west.internal.npmjs.com","tmp":"tmp/proj4-2.3.14.tgz_1457689264880_0.9409773757215589"};
	var _resolved = "https://registry.npmjs.org/proj4/-/proj4-2.3.14.tgz";
	var readme = "ERROR: No README data found!";
	var require$$1$7 = {
		name: name,
		version: version,
		description: description,
		main: main,
		directories: directories,
		scripts: scripts,
		repository: repository,
		author: author,
		license: license,
		jam: jam,
		devDependencies: devDependencies,
		dependencies: dependencies,
		contributors: contributors,
		gitHead: gitHead,
		bugs: bugs,
		homepage: homepage,
		_id: _id,
		_shasum: _shasum,
		_from: _from,
		_npmVersion: _npmVersion,
		_nodeVersion: _nodeVersion,
		_npmUser: _npmUser,
		dist: dist,
		maintainers: maintainers,
		_npmOperationalInternal: _npmOperationalInternal,
		_resolved: _resolved,
		readme: readme
	};

	var mgrs = createCommonjsModule(function (module, exports) {
	/**
	 * UTM zones are grouped, and assigned to one of a group of 6
	 * sets.
	 *
	 * {int} @private
	 */
	var NUM_100K_SETS = 6;

	/**
	 * The column letters (for easting) of the lower left value, per
	 * set.
	 *
	 * {string} @private
	 */
	var SET_ORIGIN_COLUMN_LETTERS = 'AJSAJS';

	/**
	 * The row letters (for northing) of the lower left value, per
	 * set.
	 *
	 * {string} @private
	 */
	var SET_ORIGIN_ROW_LETTERS = 'AFAFAF';

	var A = 65; // A
	var I = 73; // I
	var O = 79; // O
	var V = 86; // V
	var Z = 90; // Z

	/**
	 * Conversion of lat/lon to MGRS.
	 *
	 * @param {object} ll Object literal with lat and lon properties on a
	 *     WGS84 ellipsoid.
	 * @param {int} accuracy Accuracy in digits (5 for 1 m, 4 for 10 m, 3 for
	 *      100 m, 2 for 1000 m or 1 for 10000 m). Optional, default is 5.
	 * @return {string} the MGRS string for the given location and accuracy.
	 */
	exports.forward = function(ll, accuracy) {
	  accuracy = accuracy || 5; // default accuracy 1m
	  return encode(LLtoUTM({
	    lat: ll[1],
	    lon: ll[0]
	  }), accuracy);
	};

	/**
	 * Conversion of MGRS to lat/lon.
	 *
	 * @param {string} mgrs MGRS string.
	 * @return {array} An array with left (longitude), bottom (latitude), right
	 *     (longitude) and top (latitude) values in WGS84, representing the
	 *     bounding box for the provided MGRS reference.
	 */
	exports.inverse = function(mgrs) {
	  var bbox = UTMtoLL(decode(mgrs.toUpperCase()));
	  if (bbox.lat && bbox.lon) {
	    return [bbox.lon, bbox.lat, bbox.lon, bbox.lat];
	  }
	  return [bbox.left, bbox.bottom, bbox.right, bbox.top];
	};

	exports.toPoint = function(mgrs) {
	  var bbox = UTMtoLL(decode(mgrs.toUpperCase()));
	  if (bbox.lat && bbox.lon) {
	    return [bbox.lon, bbox.lat];
	  }
	  return [(bbox.left + bbox.right) / 2, (bbox.top + bbox.bottom) / 2];
	};
	/**
	 * Conversion from degrees to radians.
	 *
	 * @private
	 * @param {number} deg the angle in degrees.
	 * @return {number} the angle in radians.
	 */
	function degToRad(deg) {
	  return (deg * (Math.PI / 180.0));
	}

	/**
	 * Conversion from radians to degrees.
	 *
	 * @private
	 * @param {number} rad the angle in radians.
	 * @return {number} the angle in degrees.
	 */
	function radToDeg(rad) {
	  return (180.0 * (rad / Math.PI));
	}

	/**
	 * Converts a set of Longitude and Latitude co-ordinates to UTM
	 * using the WGS84 ellipsoid.
	 *
	 * @private
	 * @param {object} ll Object literal with lat and lon properties
	 *     representing the WGS84 coordinate to be converted.
	 * @return {object} Object literal containing the UTM value with easting,
	 *     northing, zoneNumber and zoneLetter properties, and an optional
	 *     accuracy property in digits. Returns null if the conversion failed.
	 */
	function LLtoUTM(ll) {
	  var Lat = ll.lat;
	  var Long = ll.lon;
	  var a = 6378137.0; //ellip.radius;
	  var eccSquared = 0.00669438; //ellip.eccsq;
	  var k0 = 0.9996;
	  var LongOrigin;
	  var eccPrimeSquared;
	  var N, T, C, A, M;
	  var LatRad = degToRad(Lat);
	  var LongRad = degToRad(Long);
	  var LongOriginRad;
	  var ZoneNumber;
	  // (int)
	  ZoneNumber = Math.floor((Long + 180) / 6) + 1;

	  //Make sure the longitude 180.00 is in Zone 60
	  if (Long === 180) {
	    ZoneNumber = 60;
	  }

	  // Special zone for Norway
	  if (Lat >= 56.0 && Lat < 64.0 && Long >= 3.0 && Long < 12.0) {
	    ZoneNumber = 32;
	  }

	  // Special zones for Svalbard
	  if (Lat >= 72.0 && Lat < 84.0) {
	    if (Long >= 0.0 && Long < 9.0) {
	      ZoneNumber = 31;
	    }
	    else if (Long >= 9.0 && Long < 21.0) {
	      ZoneNumber = 33;
	    }
	    else if (Long >= 21.0 && Long < 33.0) {
	      ZoneNumber = 35;
	    }
	    else if (Long >= 33.0 && Long < 42.0) {
	      ZoneNumber = 37;
	    }
	  }

	  LongOrigin = (ZoneNumber - 1) * 6 - 180 + 3; //+3 puts origin
	  // in middle of
	  // zone
	  LongOriginRad = degToRad(LongOrigin);

	  eccPrimeSquared = (eccSquared) / (1 - eccSquared);

	  N = a / Math.sqrt(1 - eccSquared * Math.sin(LatRad) * Math.sin(LatRad));
	  T = Math.tan(LatRad) * Math.tan(LatRad);
	  C = eccPrimeSquared * Math.cos(LatRad) * Math.cos(LatRad);
	  A = Math.cos(LatRad) * (LongRad - LongOriginRad);

	  M = a * ((1 - eccSquared / 4 - 3 * eccSquared * eccSquared / 64 - 5 * eccSquared * eccSquared * eccSquared / 256) * LatRad - (3 * eccSquared / 8 + 3 * eccSquared * eccSquared / 32 + 45 * eccSquared * eccSquared * eccSquared / 1024) * Math.sin(2 * LatRad) + (15 * eccSquared * eccSquared / 256 + 45 * eccSquared * eccSquared * eccSquared / 1024) * Math.sin(4 * LatRad) - (35 * eccSquared * eccSquared * eccSquared / 3072) * Math.sin(6 * LatRad));

	  var UTMEasting = (k0 * N * (A + (1 - T + C) * A * A * A / 6.0 + (5 - 18 * T + T * T + 72 * C - 58 * eccPrimeSquared) * A * A * A * A * A / 120.0) + 500000.0);

	  var UTMNorthing = (k0 * (M + N * Math.tan(LatRad) * (A * A / 2 + (5 - T + 9 * C + 4 * C * C) * A * A * A * A / 24.0 + (61 - 58 * T + T * T + 600 * C - 330 * eccPrimeSquared) * A * A * A * A * A * A / 720.0)));
	  if (Lat < 0.0) {
	    UTMNorthing += 10000000.0; //10000000 meter offset for
	    // southern hemisphere
	  }

	  return {
	    northing: Math.round(UTMNorthing),
	    easting: Math.round(UTMEasting),
	    zoneNumber: ZoneNumber,
	    zoneLetter: getLetterDesignator(Lat)
	  };
	}

	/**
	 * Converts UTM coords to lat/long, using the WGS84 ellipsoid. This is a convenience
	 * class where the Zone can be specified as a single string eg."60N" which
	 * is then broken down into the ZoneNumber and ZoneLetter.
	 *
	 * @private
	 * @param {object} utm An object literal with northing, easting, zoneNumber
	 *     and zoneLetter properties. If an optional accuracy property is
	 *     provided (in meters), a bounding box will be returned instead of
	 *     latitude and longitude.
	 * @return {object} An object literal containing either lat and lon values
	 *     (if no accuracy was provided), or top, right, bottom and left values
	 *     for the bounding box calculated according to the provided accuracy.
	 *     Returns null if the conversion failed.
	 */
	function UTMtoLL(utm) {

	  var UTMNorthing = utm.northing;
	  var UTMEasting = utm.easting;
	  var zoneLetter = utm.zoneLetter;
	  var zoneNumber = utm.zoneNumber;
	  // check the ZoneNummber is valid
	  if (zoneNumber < 0 || zoneNumber > 60) {
	    return null;
	  }

	  var k0 = 0.9996;
	  var a = 6378137.0; //ellip.radius;
	  var eccSquared = 0.00669438; //ellip.eccsq;
	  var eccPrimeSquared;
	  var e1 = (1 - Math.sqrt(1 - eccSquared)) / (1 + Math.sqrt(1 - eccSquared));
	  var N1, T1, C1, R1, D, M;
	  var LongOrigin;
	  var mu, phi1Rad;

	  // remove 500,000 meter offset for longitude
	  var x = UTMEasting - 500000.0;
	  var y = UTMNorthing;

	  // We must know somehow if we are in the Northern or Southern
	  // hemisphere, this is the only time we use the letter So even
	  // if the Zone letter isn't exactly correct it should indicate
	  // the hemisphere correctly
	  if (zoneLetter < 'N') {
	    y -= 10000000.0; // remove 10,000,000 meter offset used
	    // for southern hemisphere
	  }

	  // There are 60 zones with zone 1 being at West -180 to -174
	  LongOrigin = (zoneNumber - 1) * 6 - 180 + 3; // +3 puts origin
	  // in middle of
	  // zone

	  eccPrimeSquared = (eccSquared) / (1 - eccSquared);

	  M = y / k0;
	  mu = M / (a * (1 - eccSquared / 4 - 3 * eccSquared * eccSquared / 64 - 5 * eccSquared * eccSquared * eccSquared / 256));

	  phi1Rad = mu + (3 * e1 / 2 - 27 * e1 * e1 * e1 / 32) * Math.sin(2 * mu) + (21 * e1 * e1 / 16 - 55 * e1 * e1 * e1 * e1 / 32) * Math.sin(4 * mu) + (151 * e1 * e1 * e1 / 96) * Math.sin(6 * mu);
	  // double phi1 = ProjMath.radToDeg(phi1Rad);

	  N1 = a / Math.sqrt(1 - eccSquared * Math.sin(phi1Rad) * Math.sin(phi1Rad));
	  T1 = Math.tan(phi1Rad) * Math.tan(phi1Rad);
	  C1 = eccPrimeSquared * Math.cos(phi1Rad) * Math.cos(phi1Rad);
	  R1 = a * (1 - eccSquared) / Math.pow(1 - eccSquared * Math.sin(phi1Rad) * Math.sin(phi1Rad), 1.5);
	  D = x / (N1 * k0);

	  var lat = phi1Rad - (N1 * Math.tan(phi1Rad) / R1) * (D * D / 2 - (5 + 3 * T1 + 10 * C1 - 4 * C1 * C1 - 9 * eccPrimeSquared) * D * D * D * D / 24 + (61 + 90 * T1 + 298 * C1 + 45 * T1 * T1 - 252 * eccPrimeSquared - 3 * C1 * C1) * D * D * D * D * D * D / 720);
	  lat = radToDeg(lat);

	  var lon = (D - (1 + 2 * T1 + C1) * D * D * D / 6 + (5 - 2 * C1 + 28 * T1 - 3 * C1 * C1 + 8 * eccPrimeSquared + 24 * T1 * T1) * D * D * D * D * D / 120) / Math.cos(phi1Rad);
	  lon = LongOrigin + radToDeg(lon);

	  var result;
	  if (utm.accuracy) {
	    var topRight = UTMtoLL({
	      northing: utm.northing + utm.accuracy,
	      easting: utm.easting + utm.accuracy,
	      zoneLetter: utm.zoneLetter,
	      zoneNumber: utm.zoneNumber
	    });
	    result = {
	      top: topRight.lat,
	      right: topRight.lon,
	      bottom: lat,
	      left: lon
	    };
	  }
	  else {
	    result = {
	      lat: lat,
	      lon: lon
	    };
	  }
	  return result;
	}

	/**
	 * Calculates the MGRS letter designator for the given latitude.
	 *
	 * @private
	 * @param {number} lat The latitude in WGS84 to get the letter designator
	 *     for.
	 * @return {char} The letter designator.
	 */
	function getLetterDesignator(lat) {
	  //This is here as an error flag to show that the Latitude is
	  //outside MGRS limits
	  var LetterDesignator = 'Z';

	  if ((84 >= lat) && (lat >= 72)) {
	    LetterDesignator = 'X';
	  }
	  else if ((72 > lat) && (lat >= 64)) {
	    LetterDesignator = 'W';
	  }
	  else if ((64 > lat) && (lat >= 56)) {
	    LetterDesignator = 'V';
	  }
	  else if ((56 > lat) && (lat >= 48)) {
	    LetterDesignator = 'U';
	  }
	  else if ((48 > lat) && (lat >= 40)) {
	    LetterDesignator = 'T';
	  }
	  else if ((40 > lat) && (lat >= 32)) {
	    LetterDesignator = 'S';
	  }
	  else if ((32 > lat) && (lat >= 24)) {
	    LetterDesignator = 'R';
	  }
	  else if ((24 > lat) && (lat >= 16)) {
	    LetterDesignator = 'Q';
	  }
	  else if ((16 > lat) && (lat >= 8)) {
	    LetterDesignator = 'P';
	  }
	  else if ((8 > lat) && (lat >= 0)) {
	    LetterDesignator = 'N';
	  }
	  else if ((0 > lat) && (lat >= -8)) {
	    LetterDesignator = 'M';
	  }
	  else if ((-8 > lat) && (lat >= -16)) {
	    LetterDesignator = 'L';
	  }
	  else if ((-16 > lat) && (lat >= -24)) {
	    LetterDesignator = 'K';
	  }
	  else if ((-24 > lat) && (lat >= -32)) {
	    LetterDesignator = 'J';
	  }
	  else if ((-32 > lat) && (lat >= -40)) {
	    LetterDesignator = 'H';
	  }
	  else if ((-40 > lat) && (lat >= -48)) {
	    LetterDesignator = 'G';
	  }
	  else if ((-48 > lat) && (lat >= -56)) {
	    LetterDesignator = 'F';
	  }
	  else if ((-56 > lat) && (lat >= -64)) {
	    LetterDesignator = 'E';
	  }
	  else if ((-64 > lat) && (lat >= -72)) {
	    LetterDesignator = 'D';
	  }
	  else if ((-72 > lat) && (lat >= -80)) {
	    LetterDesignator = 'C';
	  }
	  return LetterDesignator;
	}

	/**
	 * Encodes a UTM location as MGRS string.
	 *
	 * @private
	 * @param {object} utm An object literal with easting, northing,
	 *     zoneLetter, zoneNumber
	 * @param {number} accuracy Accuracy in digits (1-5).
	 * @return {string} MGRS string for the given UTM location.
	 */
	function encode(utm, accuracy) {
	  // prepend with leading zeroes
	  var seasting = "00000" + utm.easting,
	    snorthing = "00000" + utm.northing;

	  return utm.zoneNumber + utm.zoneLetter + get100kID(utm.easting, utm.northing, utm.zoneNumber) + seasting.substr(seasting.length - 5, accuracy) + snorthing.substr(snorthing.length - 5, accuracy);
	}

	/**
	 * Get the two letter 100k designator for a given UTM easting,
	 * northing and zone number value.
	 *
	 * @private
	 * @param {number} easting
	 * @param {number} northing
	 * @param {number} zoneNumber
	 * @return the two letter 100k designator for the given UTM location.
	 */
	function get100kID(easting, northing, zoneNumber) {
	  var setParm = get100kSetForZone(zoneNumber);
	  var setColumn = Math.floor(easting / 100000);
	  var setRow = Math.floor(northing / 100000) % 20;
	  return getLetter100kID(setColumn, setRow, setParm);
	}

	/**
	 * Given a UTM zone number, figure out the MGRS 100K set it is in.
	 *
	 * @private
	 * @param {number} i An UTM zone number.
	 * @return {number} the 100k set the UTM zone is in.
	 */
	function get100kSetForZone(i) {
	  var setParm = i % NUM_100K_SETS;
	  if (setParm === 0) {
	    setParm = NUM_100K_SETS;
	  }

	  return setParm;
	}

	/**
	 * Get the two-letter MGRS 100k designator given information
	 * translated from the UTM northing, easting and zone number.
	 *
	 * @private
	 * @param {number} column the column index as it relates to the MGRS
	 *        100k set spreadsheet, created from the UTM easting.
	 *        Values are 1-8.
	 * @param {number} row the row index as it relates to the MGRS 100k set
	 *        spreadsheet, created from the UTM northing value. Values
	 *        are from 0-19.
	 * @param {number} parm the set block, as it relates to the MGRS 100k set
	 *        spreadsheet, created from the UTM zone. Values are from
	 *        1-60.
	 * @return two letter MGRS 100k code.
	 */
	function getLetter100kID(column, row, parm) {
	  // colOrigin and rowOrigin are the letters at the origin of the set
	  var index = parm - 1;
	  var colOrigin = SET_ORIGIN_COLUMN_LETTERS.charCodeAt(index);
	  var rowOrigin = SET_ORIGIN_ROW_LETTERS.charCodeAt(index);

	  // colInt and rowInt are the letters to build to return
	  var colInt = colOrigin + column - 1;
	  var rowInt = rowOrigin + row;
	  var rollover = false;

	  if (colInt > Z) {
	    colInt = colInt - Z + A - 1;
	    rollover = true;
	  }

	  if (colInt === I || (colOrigin < I && colInt > I) || ((colInt > I || colOrigin < I) && rollover)) {
	    colInt++;
	  }

	  if (colInt === O || (colOrigin < O && colInt > O) || ((colInt > O || colOrigin < O) && rollover)) {
	    colInt++;

	    if (colInt === I) {
	      colInt++;
	    }
	  }

	  if (colInt > Z) {
	    colInt = colInt - Z + A - 1;
	  }

	  if (rowInt > V) {
	    rowInt = rowInt - V + A - 1;
	    rollover = true;
	  }
	  else {
	    rollover = false;
	  }

	  if (((rowInt === I) || ((rowOrigin < I) && (rowInt > I))) || (((rowInt > I) || (rowOrigin < I)) && rollover)) {
	    rowInt++;
	  }

	  if (((rowInt === O) || ((rowOrigin < O) && (rowInt > O))) || (((rowInt > O) || (rowOrigin < O)) && rollover)) {
	    rowInt++;

	    if (rowInt === I) {
	      rowInt++;
	    }
	  }

	  if (rowInt > V) {
	    rowInt = rowInt - V + A - 1;
	  }

	  var twoLetter = String.fromCharCode(colInt) + String.fromCharCode(rowInt);
	  return twoLetter;
	}

	/**
	 * Decode the UTM parameters from a MGRS string.
	 *
	 * @private
	 * @param {string} mgrsString an UPPERCASE coordinate string is expected.
	 * @return {object} An object literal with easting, northing, zoneLetter,
	 *     zoneNumber and accuracy (in meters) properties.
	 */
	function decode(mgrsString) {

	  if (mgrsString && mgrsString.length === 0) {
	    throw ("MGRSPoint coverting from nothing");
	  }

	  var length = mgrsString.length;

	  var hunK = null;
	  var sb = "";
	  var testChar;
	  var i = 0;

	  // get Zone number
	  while (!(/[A-Z]/).test(testChar = mgrsString.charAt(i))) {
	    if (i >= 2) {
	      throw ("MGRSPoint bad conversion from: " + mgrsString);
	    }
	    sb += testChar;
	    i++;
	  }

	  var zoneNumber = parseInt(sb, 10);

	  if (i === 0 || i + 3 > length) {
	    // A good MGRS string has to be 4-5 digits long,
	    // ##AAA/#AAA at least.
	    throw ("MGRSPoint bad conversion from: " + mgrsString);
	  }

	  var zoneLetter = mgrsString.charAt(i++);

	  // Should we check the zone letter here? Why not.
	  if (zoneLetter <= 'A' || zoneLetter === 'B' || zoneLetter === 'Y' || zoneLetter >= 'Z' || zoneLetter === 'I' || zoneLetter === 'O') {
	    throw ("MGRSPoint zone letter " + zoneLetter + " not handled: " + mgrsString);
	  }

	  hunK = mgrsString.substring(i, i += 2);

	  var set = get100kSetForZone(zoneNumber);

	  var east100k = getEastingFromChar(hunK.charAt(0), set);
	  var north100k = getNorthingFromChar(hunK.charAt(1), set);

	  // We have a bug where the northing may be 2000000 too low.
	  // How
	  // do we know when to roll over?

	  while (north100k < getMinNorthing(zoneLetter)) {
	    north100k += 2000000;
	  }

	  // calculate the char index for easting/northing separator
	  var remainder = length - i;

	  if (remainder % 2 !== 0) {
	    throw ("MGRSPoint has to have an even number \nof digits after the zone letter and two 100km letters - front \nhalf for easting meters, second half for \nnorthing meters" + mgrsString);
	  }

	  var sep = remainder / 2;

	  var sepEasting = 0.0;
	  var sepNorthing = 0.0;
	  var accuracyBonus, sepEastingString, sepNorthingString, easting, northing;
	  if (sep > 0) {
	    accuracyBonus = 100000.0 / Math.pow(10, sep);
	    sepEastingString = mgrsString.substring(i, i + sep);
	    sepEasting = parseFloat(sepEastingString) * accuracyBonus;
	    sepNorthingString = mgrsString.substring(i + sep);
	    sepNorthing = parseFloat(sepNorthingString) * accuracyBonus;
	  }

	  easting = sepEasting + east100k;
	  northing = sepNorthing + north100k;

	  return {
	    easting: easting,
	    northing: northing,
	    zoneLetter: zoneLetter,
	    zoneNumber: zoneNumber,
	    accuracy: accuracyBonus
	  };
	}

	/**
	 * Given the first letter from a two-letter MGRS 100k zone, and given the
	 * MGRS table set for the zone number, figure out the easting value that
	 * should be added to the other, secondary easting value.
	 *
	 * @private
	 * @param {char} e The first letter from a two-letter MGRS 100´k zone.
	 * @param {number} set The MGRS table set for the zone number.
	 * @return {number} The easting value for the given letter and set.
	 */
	function getEastingFromChar(e, set) {
	  // colOrigin is the letter at the origin of the set for the
	  // column
	  var curCol = SET_ORIGIN_COLUMN_LETTERS.charCodeAt(set - 1);
	  var eastingValue = 100000.0;
	  var rewindMarker = false;

	  while (curCol !== e.charCodeAt(0)) {
	    curCol++;
	    if (curCol === I) {
	      curCol++;
	    }
	    if (curCol === O) {
	      curCol++;
	    }
	    if (curCol > Z) {
	      if (rewindMarker) {
	        throw ("Bad character: " + e);
	      }
	      curCol = A;
	      rewindMarker = true;
	    }
	    eastingValue += 100000.0;
	  }

	  return eastingValue;
	}

	/**
	 * Given the second letter from a two-letter MGRS 100k zone, and given the
	 * MGRS table set for the zone number, figure out the northing value that
	 * should be added to the other, secondary northing value. You have to
	 * remember that Northings are determined from the equator, and the vertical
	 * cycle of letters mean a 2000000 additional northing meters. This happens
	 * approx. every 18 degrees of latitude. This method does *NOT* count any
	 * additional northings. You have to figure out how many 2000000 meters need
	 * to be added for the zone letter of the MGRS coordinate.
	 *
	 * @private
	 * @param {char} n Second letter of the MGRS 100k zone
	 * @param {number} set The MGRS table set number, which is dependent on the
	 *     UTM zone number.
	 * @return {number} The northing value for the given letter and set.
	 */
	function getNorthingFromChar(n, set) {

	  if (n > 'V') {
	    throw ("MGRSPoint given invalid Northing " + n);
	  }

	  // rowOrigin is the letter at the origin of the set for the
	  // column
	  var curRow = SET_ORIGIN_ROW_LETTERS.charCodeAt(set - 1);
	  var northingValue = 0.0;
	  var rewindMarker = false;

	  while (curRow !== n.charCodeAt(0)) {
	    curRow++;
	    if (curRow === I) {
	      curRow++;
	    }
	    if (curRow === O) {
	      curRow++;
	    }
	    // fixing a bug making whole application hang in this loop
	    // when 'n' is a wrong character
	    if (curRow > V) {
	      if (rewindMarker) { // making sure that this loop ends
	        throw ("Bad character: " + n);
	      }
	      curRow = A;
	      rewindMarker = true;
	    }
	    northingValue += 100000.0;
	  }

	  return northingValue;
	}

	/**
	 * The function getMinNorthing returns the minimum northing value of a MGRS
	 * zone.
	 *
	 * Ported from Geotrans' c Lattitude_Band_Value structure table.
	 *
	 * @private
	 * @param {char} zoneLetter The MGRS zone to get the min northing for.
	 * @return {number}
	 */
	function getMinNorthing(zoneLetter) {
	  var northing;
	  switch (zoneLetter) {
	  case 'C':
	    northing = 1100000.0;
	    break;
	  case 'D':
	    northing = 2000000.0;
	    break;
	  case 'E':
	    northing = 2800000.0;
	    break;
	  case 'F':
	    northing = 3700000.0;
	    break;
	  case 'G':
	    northing = 4600000.0;
	    break;
	  case 'H':
	    northing = 5500000.0;
	    break;
	  case 'J':
	    northing = 6400000.0;
	    break;
	  case 'K':
	    northing = 7300000.0;
	    break;
	  case 'L':
	    northing = 8200000.0;
	    break;
	  case 'M':
	    northing = 9100000.0;
	    break;
	  case 'N':
	    northing = 0.0;
	    break;
	  case 'P':
	    northing = 800000.0;
	    break;
	  case 'Q':
	    northing = 1700000.0;
	    break;
	  case 'R':
	    northing = 2600000.0;
	    break;
	  case 'S':
	    northing = 3500000.0;
	    break;
	  case 'T':
	    northing = 4400000.0;
	    break;
	  case 'U':
	    northing = 5300000.0;
	    break;
	  case 'V':
	    northing = 6200000.0;
	    break;
	  case 'W':
	    northing = 7000000.0;
	    break;
	  case 'X':
	    northing = 7900000.0;
	    break;
	  default:
	    northing = -1.0;
	  }
	  if (northing >= 0.0) {
	    return northing;
	  }
	  else {
	    throw ("Invalid zone letter: " + zoneLetter);
	  }

	}
	});

	var require$$0$10 = (mgrs && typeof mgrs === 'object' && 'default' in mgrs ? mgrs['default'] : mgrs);

	var toPoint$1 = createCommonjsModule(function (module) {
	module.exports = function (array){
	  var out = {
	    x: array[0],
	    y: array[1]
	  };
	  if (array.length>2) {
	    out.z = array[2];
	  }
	  if (array.length>3) {
	    out.m = array[3];
	  }
	  return out;
	};
	});

	var require$$0$12 = (toPoint$1 && typeof toPoint$1 === 'object' && 'default' in toPoint$1 ? toPoint$1['default'] : toPoint$1);

	var datum = createCommonjsModule(function (module) {
	var HALF_PI = Math.PI/2;
	var PJD_3PARAM = 1;
	var PJD_7PARAM = 2;
	var PJD_GRIDSHIFT = 3;
	var PJD_WGS84 = 4; // WGS84 or equivalent
	var PJD_NODATUM = 5; // WGS84 or equivalent
	var SEC_TO_RAD = 4.84813681109535993589914102357e-6;
	var AD_C = 1.0026000;
	var COS_67P5 = 0.38268343236508977;
	var datum = function(proj) {
	  if (!(this instanceof datum)) {
	    return new datum(proj);
	  }
	  this.datum_type = PJD_WGS84; //default setting
	  if (!proj) {
	    return;
	  }
	  if (proj.datumCode && proj.datumCode === 'none') {
	    this.datum_type = PJD_NODATUM;
	  }

	  if (proj.datum_params) {
	    this.datum_params = proj.datum_params.map(parseFloat);
	    if (this.datum_params[0] !== 0 || this.datum_params[1] !== 0 || this.datum_params[2] !== 0) {
	      this.datum_type = PJD_3PARAM;
	    }
	    if (this.datum_params.length > 3) {
	      if (this.datum_params[3] !== 0 || this.datum_params[4] !== 0 || this.datum_params[5] !== 0 || this.datum_params[6] !== 0) {
	        this.datum_type = PJD_7PARAM;
	        this.datum_params[3] *= SEC_TO_RAD;
	        this.datum_params[4] *= SEC_TO_RAD;
	        this.datum_params[5] *= SEC_TO_RAD;
	        this.datum_params[6] = (this.datum_params[6] / 1000000.0) + 1.0;
	      }
	    }
	  }

	  // DGR 2011-03-21 : nadgrids support
	  this.datum_type = proj.grids ? PJD_GRIDSHIFT : this.datum_type;

	  this.a = proj.a; //datum object also uses these values
	  this.b = proj.b;
	  this.es = proj.es;
	  this.ep2 = proj.ep2;
	  if (this.datum_type === PJD_GRIDSHIFT) {
	    this.grids = proj.grids;
	  }
	};
	datum.prototype = {


	  /****************************************************************/
	  // cs_compare_datums()
	  //   Returns TRUE if the two datums match, otherwise FALSE.
	  compare_datums: function(dest) {
	    if (this.datum_type !== dest.datum_type) {
	      return false; // false, datums are not equal
	    }
	    else if (this.a !== dest.a || Math.abs(this.es - dest.es) > 0.000000000050) {
	      // the tolerence for es is to ensure that GRS80 and WGS84
	      // are considered identical
	      return false;
	    }
	    else if (this.datum_type === PJD_3PARAM) {
	      return (this.datum_params[0] === dest.datum_params[0] && this.datum_params[1] === dest.datum_params[1] && this.datum_params[2] === dest.datum_params[2]);
	    }
	    else if (this.datum_type === PJD_7PARAM) {
	      return (this.datum_params[0] === dest.datum_params[0] && this.datum_params[1] === dest.datum_params[1] && this.datum_params[2] === dest.datum_params[2] && this.datum_params[3] === dest.datum_params[3] && this.datum_params[4] === dest.datum_params[4] && this.datum_params[5] === dest.datum_params[5] && this.datum_params[6] === dest.datum_params[6]);
	    }
	    else if (this.datum_type === PJD_GRIDSHIFT || dest.datum_type === PJD_GRIDSHIFT) {
	      //alert("ERROR: Grid shift transformations are not implemented.");
	      //return false
	      //DGR 2012-07-29 lazy ...
	      return this.nadgrids === dest.nadgrids;
	    }
	    else {
	      return true; // datums are equal
	    }
	  }, // cs_compare_datums()

	  /*
	   * The function Convert_Geodetic_To_Geocentric converts geodetic coordinates
	   * (latitude, longitude, and height) to geocentric coordinates (X, Y, Z),
	   * according to the current ellipsoid parameters.
	   *
	   *    Latitude  : Geodetic latitude in radians                     (input)
	   *    Longitude : Geodetic longitude in radians                    (input)
	   *    Height    : Geodetic height, in meters                       (input)
	   *    X         : Calculated Geocentric X coordinate, in meters    (output)
	   *    Y         : Calculated Geocentric Y coordinate, in meters    (output)
	   *    Z         : Calculated Geocentric Z coordinate, in meters    (output)
	   *
	   */
	  geodetic_to_geocentric: function(p) {
	    var Longitude = p.x;
	    var Latitude = p.y;
	    var Height = p.z ? p.z : 0; //Z value not always supplied
	    var X; // output
	    var Y;
	    var Z;

	    var Error_Code = 0; //  GEOCENT_NO_ERROR;
	    var Rn; /*  Earth radius at location  */
	    var Sin_Lat; /*  Math.sin(Latitude)  */
	    var Sin2_Lat; /*  Square of Math.sin(Latitude)  */
	    var Cos_Lat; /*  Math.cos(Latitude)  */

	    /*
	     ** Don't blow up if Latitude is just a little out of the value
	     ** range as it may just be a rounding issue.  Also removed longitude
	     ** test, it should be wrapped by Math.cos() and Math.sin().  NFW for PROJ.4, Sep/2001.
	     */
	    if (Latitude < -HALF_PI && Latitude > -1.001 * HALF_PI) {
	      Latitude = -HALF_PI;
	    }
	    else if (Latitude > HALF_PI && Latitude < 1.001 * HALF_PI) {
	      Latitude = HALF_PI;
	    }
	    else if ((Latitude < -HALF_PI) || (Latitude > HALF_PI)) {
	      /* Latitude out of range */
	      //..reportError('geocent:lat out of range:' + Latitude);
	      return null;
	    }

	    if (Longitude > Math.PI) {
	      Longitude -= (2 * Math.PI);
	    }
	    Sin_Lat = Math.sin(Latitude);
	    Cos_Lat = Math.cos(Latitude);
	    Sin2_Lat = Sin_Lat * Sin_Lat;
	    Rn = this.a / (Math.sqrt(1.0e0 - this.es * Sin2_Lat));
	    X = (Rn + Height) * Cos_Lat * Math.cos(Longitude);
	    Y = (Rn + Height) * Cos_Lat * Math.sin(Longitude);
	    Z = ((Rn * (1 - this.es)) + Height) * Sin_Lat;

	    p.x = X;
	    p.y = Y;
	    p.z = Z;
	    return Error_Code;
	  }, // cs_geodetic_to_geocentric()


	  geocentric_to_geodetic: function(p) {
	    /* local defintions and variables */
	    /* end-criterium of loop, accuracy of sin(Latitude) */
	    var genau = 1e-12;
	    var genau2 = (genau * genau);
	    var maxiter = 30;

	    var P; /* distance between semi-minor axis and location */
	    var RR; /* distance between center and location */
	    var CT; /* sin of geocentric latitude */
	    var ST; /* cos of geocentric latitude */
	    var RX;
	    var RK;
	    var RN; /* Earth radius at location */
	    var CPHI0; /* cos of start or old geodetic latitude in iterations */
	    var SPHI0; /* sin of start or old geodetic latitude in iterations */
	    var CPHI; /* cos of searched geodetic latitude */
	    var SPHI; /* sin of searched geodetic latitude */
	    var SDPHI; /* end-criterium: addition-theorem of sin(Latitude(iter)-Latitude(iter-1)) */
	    var At_Pole; /* indicates location is in polar region */
	    var iter; /* # of continous iteration, max. 30 is always enough (s.a.) */

	    var X = p.x;
	    var Y = p.y;
	    var Z = p.z ? p.z : 0.0; //Z value not always supplied
	    var Longitude;
	    var Latitude;
	    var Height;

	    At_Pole = false;
	    P = Math.sqrt(X * X + Y * Y);
	    RR = Math.sqrt(X * X + Y * Y + Z * Z);

	    /*      special cases for latitude and longitude */
	    if (P / this.a < genau) {

	      /*  special case, if P=0. (X=0., Y=0.) */
	      At_Pole = true;
	      Longitude = 0.0;

	      /*  if (X,Y,Z)=(0.,0.,0.) then Height becomes semi-minor axis
	       *  of ellipsoid (=center of mass), Latitude becomes PI/2 */
	      if (RR / this.a < genau) {
	        Latitude = HALF_PI;
	        Height = -this.b;
	        return;
	      }
	    }
	    else {
	      /*  ellipsoidal (geodetic) longitude
	       *  interval: -PI < Longitude <= +PI */
	      Longitude = Math.atan2(Y, X);
	    }

	    /* --------------------------------------------------------------
	     * Following iterative algorithm was developped by
	     * "Institut for Erdmessung", University of Hannover, July 1988.
	     * Internet: www.ife.uni-hannover.de
	     * Iterative computation of CPHI,SPHI and Height.
	     * Iteration of CPHI and SPHI to 10**-12 radian resp.
	     * 2*10**-7 arcsec.
	     * --------------------------------------------------------------
	     */
	    CT = Z / RR;
	    ST = P / RR;
	    RX = 1.0 / Math.sqrt(1.0 - this.es * (2.0 - this.es) * ST * ST);
	    CPHI0 = ST * (1.0 - this.es) * RX;
	    SPHI0 = CT * RX;
	    iter = 0;

	    /* loop to find sin(Latitude) resp. Latitude
	     * until |sin(Latitude(iter)-Latitude(iter-1))| < genau */
	    do {
	      iter++;
	      RN = this.a / Math.sqrt(1.0 - this.es * SPHI0 * SPHI0);

	      /*  ellipsoidal (geodetic) height */
	      Height = P * CPHI0 + Z * SPHI0 - RN * (1.0 - this.es * SPHI0 * SPHI0);

	      RK = this.es * RN / (RN + Height);
	      RX = 1.0 / Math.sqrt(1.0 - RK * (2.0 - RK) * ST * ST);
	      CPHI = ST * (1.0 - RK) * RX;
	      SPHI = CT * RX;
	      SDPHI = SPHI * CPHI0 - CPHI * SPHI0;
	      CPHI0 = CPHI;
	      SPHI0 = SPHI;
	    }
	    while (SDPHI * SDPHI > genau2 && iter < maxiter);

	    /*      ellipsoidal (geodetic) latitude */
	    Latitude = Math.atan(SPHI / Math.abs(CPHI));

	    p.x = Longitude;
	    p.y = Latitude;
	    p.z = Height;
	    return p;
	  }, // cs_geocentric_to_geodetic()

	  /** Convert_Geocentric_To_Geodetic
	   * The method used here is derived from 'An Improved Algorithm for
	   * Geocentric to Geodetic Coordinate Conversion', by Ralph Toms, Feb 1996
	   */
	  geocentric_to_geodetic_noniter: function(p) {
	    var X = p.x;
	    var Y = p.y;
	    var Z = p.z ? p.z : 0; //Z value not always supplied
	    var Longitude;
	    var Latitude;
	    var Height;

	    var W; /* distance from Z axis */
	    var W2; /* square of distance from Z axis */
	    var T0; /* initial estimate of vertical component */
	    var T1; /* corrected estimate of vertical component */
	    var S0; /* initial estimate of horizontal component */
	    var S1; /* corrected estimate of horizontal component */
	    var Sin_B0; /* Math.sin(B0), B0 is estimate of Bowring aux variable */
	    var Sin3_B0; /* cube of Math.sin(B0) */
	    var Cos_B0; /* Math.cos(B0) */
	    var Sin_p1; /* Math.sin(phi1), phi1 is estimated latitude */
	    var Cos_p1; /* Math.cos(phi1) */
	    var Rn; /* Earth radius at location */
	    var Sum; /* numerator of Math.cos(phi1) */
	    var At_Pole; /* indicates location is in polar region */

	    X = parseFloat(X); // cast from string to float
	    Y = parseFloat(Y);
	    Z = parseFloat(Z);

	    At_Pole = false;
	    if (X !== 0.0) {
	      Longitude = Math.atan2(Y, X);
	    }
	    else {
	      if (Y > 0) {
	        Longitude = HALF_PI;
	      }
	      else if (Y < 0) {
	        Longitude = -HALF_PI;
	      }
	      else {
	        At_Pole = true;
	        Longitude = 0.0;
	        if (Z > 0.0) { /* north pole */
	          Latitude = HALF_PI;
	        }
	        else if (Z < 0.0) { /* south pole */
	          Latitude = -HALF_PI;
	        }
	        else { /* center of earth */
	          Latitude = HALF_PI;
	          Height = -this.b;
	          return;
	        }
	      }
	    }
	    W2 = X * X + Y * Y;
	    W = Math.sqrt(W2);
	    T0 = Z * AD_C;
	    S0 = Math.sqrt(T0 * T0 + W2);
	    Sin_B0 = T0 / S0;
	    Cos_B0 = W / S0;
	    Sin3_B0 = Sin_B0 * Sin_B0 * Sin_B0;
	    T1 = Z + this.b * this.ep2 * Sin3_B0;
	    Sum = W - this.a * this.es * Cos_B0 * Cos_B0 * Cos_B0;
	    S1 = Math.sqrt(T1 * T1 + Sum * Sum);
	    Sin_p1 = T1 / S1;
	    Cos_p1 = Sum / S1;
	    Rn = this.a / Math.sqrt(1.0 - this.es * Sin_p1 * Sin_p1);
	    if (Cos_p1 >= COS_67P5) {
	      Height = W / Cos_p1 - Rn;
	    }
	    else if (Cos_p1 <= -COS_67P5) {
	      Height = W / -Cos_p1 - Rn;
	    }
	    else {
	      Height = Z / Sin_p1 + Rn * (this.es - 1.0);
	    }
	    if (At_Pole === false) {
	      Latitude = Math.atan(Sin_p1 / Cos_p1);
	    }

	    p.x = Longitude;
	    p.y = Latitude;
	    p.z = Height;
	    return p;
	  }, // geocentric_to_geodetic_noniter()

	  /****************************************************************/
	  // pj_geocentic_to_wgs84( p )
	  //  p = point to transform in geocentric coordinates (x,y,z)
	  geocentric_to_wgs84: function(p) {

	    if (this.datum_type === PJD_3PARAM) {
	      // if( x[io] === HUGE_VAL )
	      //    continue;
	      p.x += this.datum_params[0];
	      p.y += this.datum_params[1];
	      p.z += this.datum_params[2];

	    }
	    else if (this.datum_type === PJD_7PARAM) {
	      var Dx_BF = this.datum_params[0];
	      var Dy_BF = this.datum_params[1];
	      var Dz_BF = this.datum_params[2];
	      var Rx_BF = this.datum_params[3];
	      var Ry_BF = this.datum_params[4];
	      var Rz_BF = this.datum_params[5];
	      var M_BF = this.datum_params[6];
	      // if( x[io] === HUGE_VAL )
	      //    continue;
	      var x_out = M_BF * (p.x - Rz_BF * p.y + Ry_BF * p.z) + Dx_BF;
	      var y_out = M_BF * (Rz_BF * p.x + p.y - Rx_BF * p.z) + Dy_BF;
	      var z_out = M_BF * (-Ry_BF * p.x + Rx_BF * p.y + p.z) + Dz_BF;
	      p.x = x_out;
	      p.y = y_out;
	      p.z = z_out;
	    }
	  }, // cs_geocentric_to_wgs84

	  /****************************************************************/
	  // pj_geocentic_from_wgs84()
	  //  coordinate system definition,
	  //  point to transform in geocentric coordinates (x,y,z)
	  geocentric_from_wgs84: function(p) {

	    if (this.datum_type === PJD_3PARAM) {
	      //if( x[io] === HUGE_VAL )
	      //    continue;
	      p.x -= this.datum_params[0];
	      p.y -= this.datum_params[1];
	      p.z -= this.datum_params[2];

	    }
	    else if (this.datum_type === PJD_7PARAM) {
	      var Dx_BF = this.datum_params[0];
	      var Dy_BF = this.datum_params[1];
	      var Dz_BF = this.datum_params[2];
	      var Rx_BF = this.datum_params[3];
	      var Ry_BF = this.datum_params[4];
	      var Rz_BF = this.datum_params[5];
	      var M_BF = this.datum_params[6];
	      var x_tmp = (p.x - Dx_BF) / M_BF;
	      var y_tmp = (p.y - Dy_BF) / M_BF;
	      var z_tmp = (p.z - Dz_BF) / M_BF;
	      //if( x[io] === HUGE_VAL )
	      //    continue;

	      p.x = x_tmp + Rz_BF * y_tmp - Ry_BF * z_tmp;
	      p.y = -Rz_BF * x_tmp + y_tmp + Rx_BF * z_tmp;
	      p.z = Ry_BF * x_tmp - Rx_BF * y_tmp + z_tmp;
	    } //cs_geocentric_from_wgs84()
	  }
	};

	/** point object, nothing fancy, just allows values to be
	    passed back and forth by reference rather than by value.
	    Other point classes may be used as long as they have
	    x and y properties, which will get modified in the transform method.
	*/
	module.exports = datum;
	});

	var require$$0$14 = (datum && typeof datum === 'object' && 'default' in datum ? datum['default'] : datum);

	var extend = createCommonjsModule(function (module) {
	module.exports = function(destination, source) {
	  destination = destination || {};
	  var value, property;
	  if (!source) {
	    return destination;
	  }
	  for (property in source) {
	    value = source[property];
	    if (value !== undefined) {
	      destination[property] = value;
	    }
	  }
	  return destination;
	};
	});

	var require$$0$15 = (extend && typeof extend === 'object' && 'default' in extend ? extend['default'] : extend);

	var Ellipsoid = createCommonjsModule(function (module, exports) {
	exports.MERIT = {
	  a: 6378137.0,
	  rf: 298.257,
	  ellipseName: "MERIT 1983"
	};
	exports.SGS85 = {
	  a: 6378136.0,
	  rf: 298.257,
	  ellipseName: "Soviet Geodetic System 85"
	};
	exports.GRS80 = {
	  a: 6378137.0,
	  rf: 298.257222101,
	  ellipseName: "GRS 1980(IUGG, 1980)"
	};
	exports.IAU76 = {
	  a: 6378140.0,
	  rf: 298.257,
	  ellipseName: "IAU 1976"
	};
	exports.airy = {
	  a: 6377563.396,
	  b: 6356256.910,
	  ellipseName: "Airy 1830"
	};
	exports.APL4 = {
	  a: 6378137,
	  rf: 298.25,
	  ellipseName: "Appl. Physics. 1965"
	};
	exports.NWL9D = {
	  a: 6378145.0,
	  rf: 298.25,
	  ellipseName: "Naval Weapons Lab., 1965"
	};
	exports.mod_airy = {
	  a: 6377340.189,
	  b: 6356034.446,
	  ellipseName: "Modified Airy"
	};
	exports.andrae = {
	  a: 6377104.43,
	  rf: 300.0,
	  ellipseName: "Andrae 1876 (Den., Iclnd.)"
	};
	exports.aust_SA = {
	  a: 6378160.0,
	  rf: 298.25,
	  ellipseName: "Australian Natl & S. Amer. 1969"
	};
	exports.GRS67 = {
	  a: 6378160.0,
	  rf: 298.2471674270,
	  ellipseName: "GRS 67(IUGG 1967)"
	};
	exports.bessel = {
	  a: 6377397.155,
	  rf: 299.1528128,
	  ellipseName: "Bessel 1841"
	};
	exports.bess_nam = {
	  a: 6377483.865,
	  rf: 299.1528128,
	  ellipseName: "Bessel 1841 (Namibia)"
	};
	exports.clrk66 = {
	  a: 6378206.4,
	  b: 6356583.8,
	  ellipseName: "Clarke 1866"
	};
	exports.clrk80 = {
	  a: 6378249.145,
	  rf: 293.4663,
	  ellipseName: "Clarke 1880 mod."
	};
	exports.clrk58 = {
	  a: 6378293.645208759,
	  rf: 294.2606763692654,
	  ellipseName: "Clarke 1858"
	};
	exports.CPM = {
	  a: 6375738.7,
	  rf: 334.29,
	  ellipseName: "Comm. des Poids et Mesures 1799"
	};
	exports.delmbr = {
	  a: 6376428.0,
	  rf: 311.5,
	  ellipseName: "Delambre 1810 (Belgium)"
	};
	exports.engelis = {
	  a: 6378136.05,
	  rf: 298.2566,
	  ellipseName: "Engelis 1985"
	};
	exports.evrst30 = {
	  a: 6377276.345,
	  rf: 300.8017,
	  ellipseName: "Everest 1830"
	};
	exports.evrst48 = {
	  a: 6377304.063,
	  rf: 300.8017,
	  ellipseName: "Everest 1948"
	};
	exports.evrst56 = {
	  a: 6377301.243,
	  rf: 300.8017,
	  ellipseName: "Everest 1956"
	};
	exports.evrst69 = {
	  a: 6377295.664,
	  rf: 300.8017,
	  ellipseName: "Everest 1969"
	};
	exports.evrstSS = {
	  a: 6377298.556,
	  rf: 300.8017,
	  ellipseName: "Everest (Sabah & Sarawak)"
	};
	exports.fschr60 = {
	  a: 6378166.0,
	  rf: 298.3,
	  ellipseName: "Fischer (Mercury Datum) 1960"
	};
	exports.fschr60m = {
	  a: 6378155.0,
	  rf: 298.3,
	  ellipseName: "Fischer 1960"
	};
	exports.fschr68 = {
	  a: 6378150.0,
	  rf: 298.3,
	  ellipseName: "Fischer 1968"
	};
	exports.helmert = {
	  a: 6378200.0,
	  rf: 298.3,
	  ellipseName: "Helmert 1906"
	};
	exports.hough = {
	  a: 6378270.0,
	  rf: 297.0,
	  ellipseName: "Hough"
	};
	exports.intl = {
	  a: 6378388.0,
	  rf: 297.0,
	  ellipseName: "International 1909 (Hayford)"
	};
	exports.kaula = {
	  a: 6378163.0,
	  rf: 298.24,
	  ellipseName: "Kaula 1961"
	};
	exports.lerch = {
	  a: 6378139.0,
	  rf: 298.257,
	  ellipseName: "Lerch 1979"
	};
	exports.mprts = {
	  a: 6397300.0,
	  rf: 191.0,
	  ellipseName: "Maupertius 1738"
	};
	exports.new_intl = {
	  a: 6378157.5,
	  b: 6356772.2,
	  ellipseName: "New International 1967"
	};
	exports.plessis = {
	  a: 6376523.0,
	  rf: 6355863.0,
	  ellipseName: "Plessis 1817 (France)"
	};
	exports.krass = {
	  a: 6378245.0,
	  rf: 298.3,
	  ellipseName: "Krassovsky, 1942"
	};
	exports.SEasia = {
	  a: 6378155.0,
	  b: 6356773.3205,
	  ellipseName: "Southeast Asia"
	};
	exports.walbeck = {
	  a: 6376896.0,
	  b: 6355834.8467,
	  ellipseName: "Walbeck"
	};
	exports.WGS60 = {
	  a: 6378165.0,
	  rf: 298.3,
	  ellipseName: "WGS 60"
	};
	exports.WGS66 = {
	  a: 6378145.0,
	  rf: 298.25,
	  ellipseName: "WGS 66"
	};
	exports.WGS7 = {
	  a: 6378135.0,
	  rf: 298.26,
	  ellipseName: "WGS 72"
	};
	exports.WGS84 = {
	  a: 6378137.0,
	  rf: 298.257223563,
	  ellipseName: "WGS 84"
	};
	exports.sphere = {
	  a: 6370997.0,
	  b: 6370997.0,
	  ellipseName: "Normal Sphere (r=6370997)"
	};
	});

	var require$$2$2 = (Ellipsoid && typeof Ellipsoid === 'object' && 'default' in Ellipsoid ? Ellipsoid['default'] : Ellipsoid);

	var Datum = createCommonjsModule(function (module, exports) {
	exports.wgs84 = {
	  towgs84: "0,0,0",
	  ellipse: "WGS84",
	  datumName: "WGS84"
	};
	exports.ch1903 = {
	  towgs84: "674.374,15.056,405.346",
	  ellipse: "bessel",
	  datumName: "swiss"
	};
	exports.ggrs87 = {
	  towgs84: "-199.87,74.79,246.62",
	  ellipse: "GRS80",
	  datumName: "Greek_Geodetic_Reference_System_1987"
	};
	exports.nad83 = {
	  towgs84: "0,0,0",
	  ellipse: "GRS80",
	  datumName: "North_American_Datum_1983"
	};
	exports.nad27 = {
	  nadgrids: "@conus,@alaska,@ntv2_0.gsb,@ntv1_can.dat",
	  ellipse: "clrk66",
	  datumName: "North_American_Datum_1927"
	};
	exports.potsdam = {
	  towgs84: "606.0,23.0,413.0",
	  ellipse: "bessel",
	  datumName: "Potsdam Rauenberg 1950 DHDN"
	};
	exports.carthage = {
	  towgs84: "-263.0,6.0,431.0",
	  ellipse: "clark80",
	  datumName: "Carthage 1934 Tunisia"
	};
	exports.hermannskogel = {
	  towgs84: "653.0,-212.0,449.0",
	  ellipse: "bessel",
	  datumName: "Hermannskogel"
	};
	exports.ire65 = {
	  towgs84: "482.530,-130.596,564.557,-1.042,-0.214,-0.631,8.15",
	  ellipse: "mod_airy",
	  datumName: "Ireland 1965"
	};
	exports.rassadiran = {
	  towgs84: "-133.63,-157.5,-158.62",
	  ellipse: "intl",
	  datumName: "Rassadiran"
	};
	exports.nzgd49 = {
	  towgs84: "59.47,-5.04,187.44,0.47,-0.1,1.024,-4.5993",
	  ellipse: "intl",
	  datumName: "New Zealand Geodetic Datum 1949"
	};
	exports.osgb36 = {
	  towgs84: "446.448,-125.157,542.060,0.1502,0.2470,0.8421,-20.4894",
	  ellipse: "airy",
	  datumName: "Airy 1830"
	};
	exports.s_jtsk = {
	  towgs84: "589,76,480",
	  ellipse: 'bessel',
	  datumName: 'S-JTSK (Ferro)'
	};
	exports.beduaram = {
	  towgs84: '-106,-87,188',
	  ellipse: 'clrk80',
	  datumName: 'Beduaram'
	};
	exports.gunung_segara = {
	  towgs84: '-403,684,41',
	  ellipse: 'bessel',
	  datumName: 'Gunung Segara Jakarta'
	};
	exports.rnb72 = {
	  towgs84: "106.869,-52.2978,103.724,-0.33657,0.456955,-1.84218,1",
	  ellipse: "intl",
	  datumName: "Reseau National Belge 1972"
	};
	});

	var require$$3$5 = (Datum && typeof Datum === 'object' && 'default' in Datum ? Datum['default'] : Datum);

	var deriveConstants = createCommonjsModule(function (module) {
	var Datum = require$$3$5;
	var Ellipsoid = require$$2$2;
	var extend = require$$0$15;
	var datum = require$$0$14;
	var EPSLN = 1.0e-10;
	// ellipoid pj_set_ell.c
	var SIXTH = 0.1666666666666666667;
	/* 1/6 */
	var RA4 = 0.04722222222222222222;
	/* 17/360 */
	var RA6 = 0.02215608465608465608;
	module.exports = function(json) {
	  // DGR 2011-03-20 : nagrids -> nadgrids
	  if (json.datumCode && json.datumCode !== 'none') {
	    var datumDef = Datum[json.datumCode];
	    if (datumDef) {
	      json.datum_params = datumDef.towgs84 ? datumDef.towgs84.split(',') : null;
	      json.ellps = datumDef.ellipse;
	      json.datumName = datumDef.datumName ? datumDef.datumName : json.datumCode;
	    }
	  }
	  if (!json.a) { // do we have an ellipsoid?
	    var ellipse = Ellipsoid[json.ellps] ? Ellipsoid[json.ellps] : Ellipsoid.WGS84;
	    extend(json, ellipse);
	  }
	  if (json.rf && !json.b) {
	    json.b = (1.0 - 1.0 / json.rf) * json.a;
	  }
	  if (json.rf === 0 || Math.abs(json.a - json.b) < EPSLN) {
	    json.sphere = true;
	    json.b = json.a;
	  }
	  json.a2 = json.a * json.a; // used in geocentric
	  json.b2 = json.b * json.b; // used in geocentric
	  json.es = (json.a2 - json.b2) / json.a2; // e ^ 2
	  json.e = Math.sqrt(json.es); // eccentricity
	  if (json.R_A) {
	    json.a *= 1 - json.es * (SIXTH + json.es * (RA4 + json.es * RA6));
	    json.a2 = json.a * json.a;
	    json.b2 = json.b * json.b;
	    json.es = 0;
	  }
	  json.ep2 = (json.a2 - json.b2) / json.b2; // used in geocentric
	  if (!json.k0) {
	    json.k0 = 1.0; //default value
	  }
	  //DGR 2010-11-12: axis
	  if (!json.axis) {
	    json.axis = "enu";
	  }

	  if (!json.datum) {
	    json.datum = datum(json);
	  }
	  return json;
	};
	});

	var require$$0$13 = (deriveConstants && typeof deriveConstants === 'object' && 'default' in deriveConstants ? deriveConstants['default'] : deriveConstants);

	var longlat = createCommonjsModule(function (module, exports) {
	exports.init = function() {
	  //no-op for longlat
	};

	function identity(pt) {
	  return pt;
	}
	exports.forward = identity;
	exports.inverse = identity;
	exports.names = ["longlat", "identity"];
	});

	var require$$0$16 = (longlat && typeof longlat === 'object' && 'default' in longlat ? longlat['default'] : longlat);

	var merc = createCommonjsModule(function (module, exports) {
	var msfnz = require$$3$2;
	var HALF_PI = Math.PI/2;
	var EPSLN = 1.0e-10;
	var R2D = 57.29577951308232088;
	var adjust_lon = require$$2;
	var FORTPI = Math.PI/4;
	var tsfnz = require$$1$5;
	var phi2z = require$$0$7;
	exports.init = function() {
	  var con = this.b / this.a;
	  this.es = 1 - con * con;
	  if(!('x0' in this)){
	    this.x0 = 0;
	  }
	  if(!('y0' in this)){
	    this.y0 = 0;
	  }
	  this.e = Math.sqrt(this.es);
	  if (this.lat_ts) {
	    if (this.sphere) {
	      this.k0 = Math.cos(this.lat_ts);
	    }
	    else {
	      this.k0 = msfnz(this.e, Math.sin(this.lat_ts), Math.cos(this.lat_ts));
	    }
	  }
	  else {
	    if (!this.k0) {
	      if (this.k) {
	        this.k0 = this.k;
	      }
	      else {
	        this.k0 = 1;
	      }
	    }
	  }
	};

	/* Mercator forward equations--mapping lat,long to x,y
	  --------------------------------------------------*/

	exports.forward = function(p) {
	  var lon = p.x;
	  var lat = p.y;
	  // convert to radians
	  if (lat * R2D > 90 && lat * R2D < -90 && lon * R2D > 180 && lon * R2D < -180) {
	    return null;
	  }

	  var x, y;
	  if (Math.abs(Math.abs(lat) - HALF_PI) <= EPSLN) {
	    return null;
	  }
	  else {
	    if (this.sphere) {
	      x = this.x0 + this.a * this.k0 * adjust_lon(lon - this.long0);
	      y = this.y0 + this.a * this.k0 * Math.log(Math.tan(FORTPI + 0.5 * lat));
	    }
	    else {
	      var sinphi = Math.sin(lat);
	      var ts = tsfnz(this.e, lat, sinphi);
	      x = this.x0 + this.a * this.k0 * adjust_lon(lon - this.long0);
	      y = this.y0 - this.a * this.k0 * Math.log(ts);
	    }
	    p.x = x;
	    p.y = y;
	    return p;
	  }
	};


	/* Mercator inverse equations--mapping x,y to lat/long
	  --------------------------------------------------*/
	exports.inverse = function(p) {

	  var x = p.x - this.x0;
	  var y = p.y - this.y0;
	  var lon, lat;

	  if (this.sphere) {
	    lat = HALF_PI - 2 * Math.atan(Math.exp(-y / (this.a * this.k0)));
	  }
	  else {
	    var ts = Math.exp(-y / (this.a * this.k0));
	    lat = phi2z(this.e, ts);
	    if (lat === -9999) {
	      return null;
	    }
	  }
	  lon = adjust_lon(this.long0 + x / (this.a * this.k0));

	  p.x = lon;
	  p.y = lat;
	  return p;
	};

	exports.names = ["Mercator", "Popular Visualisation Pseudo Mercator", "Mercator_1SP", "Mercator_Auxiliary_Sphere", "merc"];
	});

	var require$$1$10 = (merc && typeof merc === 'object' && 'default' in merc ? merc['default'] : merc);

	var projections = createCommonjsModule(function (module, exports) {
	var projs = [
	  require$$1$10,
	  require$$0$16
	];
	var names = {};
	var projStore = [];

	function add(proj, i) {
	  var len = projStore.length;
	  if (!proj.names) {
	    console.log(i);
	    return true;
	  }
	  projStore[len] = proj;
	  proj.names.forEach(function(n) {
	    names[n.toLowerCase()] = len;
	  });
	  return this;
	}

	exports.add = add;

	exports.get = function(name) {
	  if (!name) {
	    return false;
	  }
	  var n = name.toLowerCase();
	  if (typeof names[n] !== 'undefined' && projStore[names[n]]) {
	    return projStore[names[n]];
	  }
	};
	exports.start = function() {
	  projs.forEach(add);
	};
	});

	var require$$1$9 = (projections && typeof projections === 'object' && 'default' in projections ? projections['default'] : projections);

	var units = createCommonjsModule(function (module, exports) {
	exports.ft = {to_meter: 0.3048};
	exports['us-ft'] = {to_meter: 1200 / 3937};
	});

	var require$$0$17 = (units && typeof units === 'object' && 'default' in units ? units['default'] : units);

	var PrimeMeridian = createCommonjsModule(function (module, exports) {
	exports.greenwich = 0.0; //"0dE",
	exports.lisbon = -9.131906111111; //"9d07'54.862\"W",
	exports.paris = 2.337229166667; //"2d20'14.025\"E",
	exports.bogota = -74.080916666667; //"74d04'51.3\"W",
	exports.madrid = -3.687938888889; //"3d41'16.58\"W",
	exports.rome = 12.452333333333; //"12d27'8.4\"E",
	exports.bern = 7.439583333333; //"7d26'22.5\"E",
	exports.jakarta = 106.807719444444; //"106d48'27.79\"E",
	exports.ferro = -17.666666666667; //"17d40'W",
	exports.brussels = 4.367975; //"4d22'4.71\"E",
	exports.stockholm = 18.058277777778; //"18d3'29.8\"E",
	exports.athens = 23.7163375; //"23d42'58.815\"E",
	exports.oslo = 10.722916666667; //"10d43'22.5\"E"
	});

	var require$$1$12 = (PrimeMeridian && typeof PrimeMeridian === 'object' && 'default' in PrimeMeridian ? PrimeMeridian['default'] : PrimeMeridian);

	var projString = createCommonjsModule(function (module) {
	var D2R = 0.01745329251994329577;
	var PrimeMeridian = require$$1$12;
	var units = require$$0$17;

	module.exports = function(defData) {
	  var self = {};
	  var paramObj = {};
	  defData.split("+").map(function(v) {
	    return v.trim();
	  }).filter(function(a) {
	    return a;
	  }).forEach(function(a) {
	    var split = a.split("=");
	    split.push(true);
	    paramObj[split[0].toLowerCase()] = split[1];
	  });
	  var paramName, paramVal, paramOutname;
	  var params = {
	    proj: 'projName',
	    datum: 'datumCode',
	    rf: function(v) {
	      self.rf = parseFloat(v);
	    },
	    lat_0: function(v) {
	      self.lat0 = v * D2R;
	    },
	    lat_1: function(v) {
	      self.lat1 = v * D2R;
	    },
	    lat_2: function(v) {
	      self.lat2 = v * D2R;
	    },
	    lat_ts: function(v) {
	      self.lat_ts = v * D2R;
	    },
	    lon_0: function(v) {
	      self.long0 = v * D2R;
	    },
	    lon_1: function(v) {
	      self.long1 = v * D2R;
	    },
	    lon_2: function(v) {
	      self.long2 = v * D2R;
	    },
	    alpha: function(v) {
	      self.alpha = parseFloat(v) * D2R;
	    },
	    lonc: function(v) {
	      self.longc = v * D2R;
	    },
	    x_0: function(v) {
	      self.x0 = parseFloat(v);
	    },
	    y_0: function(v) {
	      self.y0 = parseFloat(v);
	    },
	    k_0: function(v) {
	      self.k0 = parseFloat(v);
	    },
	    k: function(v) {
	      self.k0 = parseFloat(v);
	    },
	    a: function(v) {
	      self.a = parseFloat(v);
	    },
	    b: function(v) {
	      self.b = parseFloat(v);
	    },
	    r_a: function() {
	      self.R_A = true;
	    },
	    zone: function(v) {
	      self.zone = parseInt(v, 10);
	    },
	    south: function() {
	      self.utmSouth = true;
	    },
	    towgs84: function(v) {
	      self.datum_params = v.split(",").map(function(a) {
	        return parseFloat(a);
	      });
	    },
	    to_meter: function(v) {
	      self.to_meter = parseFloat(v);
	    },
	    units: function(v) {
	      self.units = v;
	      if (units[v]) {
	        self.to_meter = units[v].to_meter;
	      }
	    },
	    from_greenwich: function(v) {
	      self.from_greenwich = v * D2R;
	    },
	    pm: function(v) {
	      self.from_greenwich = (PrimeMeridian[v] ? PrimeMeridian[v] : parseFloat(v)) * D2R;
	    },
	    nadgrids: function(v) {
	      if (v === '@null') {
	        self.datumCode = 'none';
	      }
	      else {
	        self.nadgrids = v;
	      }
	    },
	    axis: function(v) {
	      var legalAxis = "ewnsud";
	      if (v.length === 3 && legalAxis.indexOf(v.substr(0, 1)) !== -1 && legalAxis.indexOf(v.substr(1, 1)) !== -1 && legalAxis.indexOf(v.substr(2, 1)) !== -1) {
	        self.axis = v;
	      }
	    }
	  };
	  for (paramName in paramObj) {
	    paramVal = paramObj[paramName];
	    if (paramName in params) {
	      paramOutname = params[paramName];
	      if (typeof paramOutname === 'function') {
	        paramOutname(paramVal);
	      }
	      else {
	        self[paramOutname] = paramVal;
	      }
	    }
	    else {
	      self[paramName] = paramVal;
	    }
	  }
	  if(typeof self.datumCode === 'string' && self.datumCode !== "WGS84"){
	    self.datumCode = self.datumCode.toLowerCase();
	  }
	  return self;
	};
	});

	var require$$1$11 = (projString && typeof projString === 'object' && 'default' in projString ? projString['default'] : projString);

	var wkt = createCommonjsModule(function (module) {
	var D2R = 0.01745329251994329577;
	var extend = require$$0$15;

	function mapit(obj, key, v) {
	  obj[key] = v.map(function(aa) {
	    var o = {};
	    sExpr(aa, o);
	    return o;
	  }).reduce(function(a, b) {
	    return extend(a, b);
	  }, {});
	}

	function sExpr(v, obj) {
	  var key;
	  if (!Array.isArray(v)) {
	    obj[v] = true;
	    return;
	  }
	  else {
	    key = v.shift();
	    if (key === 'PARAMETER') {
	      key = v.shift();
	    }
	    if (v.length === 1) {
	      if (Array.isArray(v[0])) {
	        obj[key] = {};
	        sExpr(v[0], obj[key]);
	      }
	      else {
	        obj[key] = v[0];
	      }
	    }
	    else if (!v.length) {
	      obj[key] = true;
	    }
	    else if (key === 'TOWGS84') {
	      obj[key] = v;
	    }
	    else {
	      obj[key] = {};
	      if (['UNIT', 'PRIMEM', 'VERT_DATUM'].indexOf(key) > -1) {
	        obj[key] = {
	          name: v[0].toLowerCase(),
	          convert: v[1]
	        };
	        if (v.length === 3) {
	          obj[key].auth = v[2];
	        }
	      }
	      else if (key === 'SPHEROID') {
	        obj[key] = {
	          name: v[0],
	          a: v[1],
	          rf: v[2]
	        };
	        if (v.length === 4) {
	          obj[key].auth = v[3];
	        }
	      }
	      else if (['GEOGCS', 'GEOCCS', 'DATUM', 'VERT_CS', 'COMPD_CS', 'LOCAL_CS', 'FITTED_CS', 'LOCAL_DATUM'].indexOf(key) > -1) {
	        v[0] = ['name', v[0]];
	        mapit(obj, key, v);
	      }
	      else if (v.every(function(aa) {
	        return Array.isArray(aa);
	      })) {
	        mapit(obj, key, v);
	      }
	      else {
	        sExpr(v, obj[key]);
	      }
	    }
	  }
	}

	function rename(obj, params) {
	  var outName = params[0];
	  var inName = params[1];
	  if (!(outName in obj) && (inName in obj)) {
	    obj[outName] = obj[inName];
	    if (params.length === 3) {
	      obj[outName] = params[2](obj[outName]);
	    }
	  }
	}

	function d2r(input) {
	  return input * D2R;
	}

	function cleanWKT(wkt) {
	  if (wkt.type === 'GEOGCS') {
	    wkt.projName = 'longlat';
	  }
	  else if (wkt.type === 'LOCAL_CS') {
	    wkt.projName = 'identity';
	    wkt.local = true;
	  }
	  else {
	    if (typeof wkt.PROJECTION === "object") {
	      wkt.projName = Object.keys(wkt.PROJECTION)[0];
	    }
	    else {
	      wkt.projName = wkt.PROJECTION;
	    }
	  }
	  if (wkt.UNIT) {
	    wkt.units = wkt.UNIT.name.toLowerCase();
	    if (wkt.units === 'metre') {
	      wkt.units = 'meter';
	    }
	    if (wkt.UNIT.convert) {
	      if (wkt.type === 'GEOGCS') {
	        if (wkt.DATUM && wkt.DATUM.SPHEROID) {
	          wkt.to_meter = parseFloat(wkt.UNIT.convert, 10)*wkt.DATUM.SPHEROID.a;
	        }
	      } else {
	        wkt.to_meter = parseFloat(wkt.UNIT.convert, 10);
	      }
	    }
	  }

	  if (wkt.GEOGCS) {
	    //if(wkt.GEOGCS.PRIMEM&&wkt.GEOGCS.PRIMEM.convert){
	    //  wkt.from_greenwich=wkt.GEOGCS.PRIMEM.convert*D2R;
	    //}
	    if (wkt.GEOGCS.DATUM) {
	      wkt.datumCode = wkt.GEOGCS.DATUM.name.toLowerCase();
	    }
	    else {
	      wkt.datumCode = wkt.GEOGCS.name.toLowerCase();
	    }
	    if (wkt.datumCode.slice(0, 2) === 'd_') {
	      wkt.datumCode = wkt.datumCode.slice(2);
	    }
	    if (wkt.datumCode === 'new_zealand_geodetic_datum_1949' || wkt.datumCode === 'new_zealand_1949') {
	      wkt.datumCode = 'nzgd49';
	    }
	    if (wkt.datumCode === "wgs_1984") {
	      if (wkt.PROJECTION === 'Mercator_Auxiliary_Sphere') {
	        wkt.sphere = true;
	      }
	      wkt.datumCode = 'wgs84';
	    }
	    if (wkt.datumCode.slice(-6) === '_ferro') {
	      wkt.datumCode = wkt.datumCode.slice(0, - 6);
	    }
	    if (wkt.datumCode.slice(-8) === '_jakarta') {
	      wkt.datumCode = wkt.datumCode.slice(0, - 8);
	    }
	    if (~wkt.datumCode.indexOf('belge')) {
	      wkt.datumCode = "rnb72";
	    }
	    if (wkt.GEOGCS.DATUM && wkt.GEOGCS.DATUM.SPHEROID) {
	      wkt.ellps = wkt.GEOGCS.DATUM.SPHEROID.name.replace('_19', '').replace(/[Cc]larke\_18/, 'clrk');
	      if (wkt.ellps.toLowerCase().slice(0, 13) === "international") {
	        wkt.ellps = 'intl';
	      }

	      wkt.a = wkt.GEOGCS.DATUM.SPHEROID.a;
	      wkt.rf = parseFloat(wkt.GEOGCS.DATUM.SPHEROID.rf, 10);
	    }
	    if (~wkt.datumCode.indexOf('osgb_1936')) {
	      wkt.datumCode = "osgb36";
	    }
	  }
	  if (wkt.b && !isFinite(wkt.b)) {
	    wkt.b = wkt.a;
	  }

	  function toMeter(input) {
	    var ratio = wkt.to_meter || 1;
	    return parseFloat(input, 10) * ratio;
	  }
	  var renamer = function(a) {
	    return rename(wkt, a);
	  };
	  var list = [
	    ['standard_parallel_1', 'Standard_Parallel_1'],
	    ['standard_parallel_2', 'Standard_Parallel_2'],
	    ['false_easting', 'False_Easting'],
	    ['false_northing', 'False_Northing'],
	    ['central_meridian', 'Central_Meridian'],
	    ['latitude_of_origin', 'Latitude_Of_Origin'],
	    ['latitude_of_origin', 'Central_Parallel'],
	    ['scale_factor', 'Scale_Factor'],
	    ['k0', 'scale_factor'],
	    ['latitude_of_center', 'Latitude_of_center'],
	    ['lat0', 'latitude_of_center', d2r],
	    ['longitude_of_center', 'Longitude_Of_Center'],
	    ['longc', 'longitude_of_center', d2r],
	    ['x0', 'false_easting', toMeter],
	    ['y0', 'false_northing', toMeter],
	    ['long0', 'central_meridian', d2r],
	    ['lat0', 'latitude_of_origin', d2r],
	    ['lat0', 'standard_parallel_1', d2r],
	    ['lat1', 'standard_parallel_1', d2r],
	    ['lat2', 'standard_parallel_2', d2r],
	    ['alpha', 'azimuth', d2r],
	    ['srsCode', 'name']
	  ];
	  list.forEach(renamer);
	  if (!wkt.long0 && wkt.longc && (wkt.projName === 'Albers_Conic_Equal_Area' || wkt.projName === "Lambert_Azimuthal_Equal_Area")) {
	    wkt.long0 = wkt.longc;
	  }
	  if (!wkt.lat_ts && wkt.lat1 && (wkt.projName === 'Stereographic_South_Pole' || wkt.projName === 'Polar Stereographic (variant B)')) {
	    wkt.lat0 = d2r(wkt.lat1 > 0 ? 90 : -90);
	    wkt.lat_ts = wkt.lat1;
	  }
	}
	module.exports = function(wkt, self) {
	  var lisp = JSON.parse(("," + wkt).replace(/\s*\,\s*([A-Z_0-9]+?)(\[)/g, ',["$1",').slice(1).replace(/\s*\,\s*([A-Z_0-9]+?)\]/g, ',"$1"]').replace(/,\["VERTCS".+/,''));
	  var type = lisp.shift();
	  var name = lisp.shift();
	  lisp.unshift(['name', name]);
	  lisp.unshift(['type', type]);
	  lisp.unshift('output');
	  var obj = {};
	  sExpr(lisp, obj);
	  cleanWKT(obj.output);
	  return extend(self, obj.output);
	};
	});

	var require$$0$18 = (wkt && typeof wkt === 'object' && 'default' in wkt ? wkt['default'] : wkt);

	var global$1 = createCommonjsModule(function (module) {
	module.exports = function(defs) {
	  defs('EPSG:4326', "+title=WGS 84 (long/lat) +proj=longlat +ellps=WGS84 +datum=WGS84 +units=degrees");
	  defs('EPSG:4269', "+title=NAD83 (long/lat) +proj=longlat +a=6378137.0 +b=6356752.31414036 +ellps=GRS80 +datum=NAD83 +units=degrees");
	  defs('EPSG:3857', "+title=WGS 84 / Pseudo-Mercator +proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +no_defs");

	  defs.WGS84 = defs['EPSG:4326'];
	  defs['EPSG:3785'] = defs['EPSG:3857']; // maintain backward compat, official code is 3857
	  defs.GOOGLE = defs['EPSG:3857'];
	  defs['EPSG:900913'] = defs['EPSG:3857'];
	  defs['EPSG:102113'] = defs['EPSG:3857'];
	};
	});

	var require$$2$4 = (global$1 && typeof global$1 === 'object' && 'default' in global$1 ? global$1['default'] : global$1);

	var defs = createCommonjsModule(function (module) {
	var globals = require$$2$4;
	var parseProj = require$$1$11;
	var wkt = require$$0$18;

	function defs(name) {
	  /*global console*/
	  var that = this;
	  if (arguments.length === 2) {
	    var def = arguments[1];
	    if (typeof def === 'string') {
	      if (def.charAt(0) === '+') {
	        defs[name] = parseProj(arguments[1]);
	      }
	      else {
	        defs[name] = wkt(arguments[1]);
	      }
	    } else {
	      defs[name] = def;
	    }
	  }
	  else if (arguments.length === 1) {
	    if (Array.isArray(name)) {
	      return name.map(function(v) {
	        if (Array.isArray(v)) {
	          defs.apply(that, v);
	        }
	        else {
	          defs(v);
	        }
	      });
	    }
	    else if (typeof name === 'string') {
	      if (name in defs) {
	        return defs[name];
	      }
	    }
	    else if ('EPSG' in name) {
	      defs['EPSG:' + name.EPSG] = name;
	    }
	    else if ('ESRI' in name) {
	      defs['ESRI:' + name.ESRI] = name;
	    }
	    else if ('IAU2000' in name) {
	      defs['IAU2000:' + name.IAU2000] = name;
	    }
	    else {
	      console.log(name);
	    }
	    return;
	  }


	}
	globals(defs);
	module.exports = defs;
	});

	var require$$2$3 = (defs && typeof defs === 'object' && 'default' in defs ? defs['default'] : defs);

	var parseCode = createCommonjsModule(function (module) {
	var defs = require$$2$3;
	var wkt = require$$0$18;
	var projStr = require$$1$11;
	function testObj(code){
	  return typeof code === 'string';
	}
	function testDef(code){
	  return code in defs;
	}
	function testWKT(code){
	  var codeWords = ['GEOGCS','GEOCCS','PROJCS','LOCAL_CS'];
	  return codeWords.reduce(function(a,b){
	    return a+1+code.indexOf(b);
	  },0);
	}
	function testProj(code){
	  return code[0] === '+';
	}
	function parse(code){
	  if (testObj(code)) {
	    //check to see if this is a WKT string
	    if (testDef(code)) {
	      return defs[code];
	    }
	    else if (testWKT(code)) {
	      return wkt(code);
	    }
	    else if (testProj(code)) {
	      return projStr(code);
	    }
	  }else{
	    return code;
	  }
	}

	module.exports = parse;
	});

	var require$$3$6 = (parseCode && typeof parseCode === 'object' && 'default' in parseCode ? parseCode['default'] : parseCode);

	var Proj = createCommonjsModule(function (module) {
	var parseCode = require$$3$6;
	var extend = require$$0$15;
	var projections = require$$1$9;
	var deriveConstants = require$$0$13;

	function Projection(srsCode,callback) {
	  if (!(this instanceof Projection)) {
	    return new Projection(srsCode);
	  }
	  callback = callback || function(error){
	    if(error){
	      throw error;
	    }
	  };
	  var json = parseCode(srsCode);
	  if(typeof json !== 'object'){
	    callback(srsCode);
	    return;
	  }
	  var modifiedJSON = deriveConstants(json);
	  var ourProj = Projection.projections.get(modifiedJSON.projName);
	  if(ourProj){
	    extend(this, modifiedJSON);
	    extend(this, ourProj);
	    this.init();
	    callback(null, this);
	  }else{
	    callback(srsCode);
	  }
	}
	Projection.projections = projections;
	Projection.projections.start();
	module.exports = Projection;
	});

	var require$$1$8 = (Proj && typeof Proj === 'object' && 'default' in Proj ? Proj['default'] : Proj);

	var adjust_axis = createCommonjsModule(function (module) {
	module.exports = function(crs, denorm, point) {
	  var xin = point.x,
	    yin = point.y,
	    zin = point.z || 0.0;
	  var v, t, i;
	  for (i = 0; i < 3; i++) {
	    if (denorm && i === 2 && point.z === undefined) {
	      continue;
	    }
	    if (i === 0) {
	      v = xin;
	      t = 'x';
	    }
	    else if (i === 1) {
	      v = yin;
	      t = 'y';
	    }
	    else {
	      v = zin;
	      t = 'z';
	    }
	    switch (crs.axis[i]) {
	    case 'e':
	      point[t] = v;
	      break;
	    case 'w':
	      point[t] = -v;
	      break;
	    case 'n':
	      point[t] = v;
	      break;
	    case 's':
	      point[t] = -v;
	      break;
	    case 'u':
	      if (point[t] !== undefined) {
	        point.z = v;
	      }
	      break;
	    case 'd':
	      if (point[t] !== undefined) {
	        point.z = -v;
	      }
	      break;
	    default:
	      //console.log("ERROR: unknow axis ("+crs.axis[i]+") - check definition of "+crs.projName);
	      return null;
	    }
	  }
	  return point;
	};
	});

	var require$$2$5 = (adjust_axis && typeof adjust_axis === 'object' && 'default' in adjust_axis ? adjust_axis['default'] : adjust_axis);

	var datum_transform = createCommonjsModule(function (module) {
	var PJD_3PARAM = 1;
	var PJD_7PARAM = 2;
	var PJD_GRIDSHIFT = 3;
	var PJD_NODATUM = 5; // WGS84 or equivalent
	var SRS_WGS84_SEMIMAJOR = 6378137; // only used in grid shift transforms
	var SRS_WGS84_ESQUARED = 0.006694379990141316; //DGR: 2012-07-29
	module.exports = function(source, dest, point) {
	  var wp, i, l;

	  function checkParams(fallback) {
	    return (fallback === PJD_3PARAM || fallback === PJD_7PARAM);
	  }
	  // Short cut if the datums are identical.
	  if (source.compare_datums(dest)) {
	    return point; // in this case, zero is sucess,
	    // whereas cs_compare_datums returns 1 to indicate TRUE
	    // confusing, should fix this
	  }

	  // Explicitly skip datum transform by setting 'datum=none' as parameter for either source or dest
	  if (source.datum_type === PJD_NODATUM || dest.datum_type === PJD_NODATUM) {
	    return point;
	  }

	  //DGR: 2012-07-29 : add nadgrids support (begin)
	  var src_a = source.a;
	  var src_es = source.es;

	  var dst_a = dest.a;
	  var dst_es = dest.es;

	  var fallback = source.datum_type;
	  // If this datum requires grid shifts, then apply it to geodetic coordinates.
	  if (fallback === PJD_GRIDSHIFT) {
	    if (this.apply_gridshift(source, 0, point) === 0) {
	      source.a = SRS_WGS84_SEMIMAJOR;
	      source.es = SRS_WGS84_ESQUARED;
	    }
	    else {
	      // try 3 or 7 params transformation or nothing ?
	      if (!source.datum_params) {
	        source.a = src_a;
	        source.es = source.es;
	        return point;
	      }
	      wp = 1;
	      for (i = 0, l = source.datum_params.length; i < l; i++) {
	        wp *= source.datum_params[i];
	      }
	      if (wp === 0) {
	        source.a = src_a;
	        source.es = source.es;
	        return point;
	      }
	      if (source.datum_params.length > 3) {
	        fallback = PJD_7PARAM;
	      }
	      else {
	        fallback = PJD_3PARAM;
	      }
	    }
	  }
	  if (dest.datum_type === PJD_GRIDSHIFT) {
	    dest.a = SRS_WGS84_SEMIMAJOR;
	    dest.es = SRS_WGS84_ESQUARED;
	  }
	  // Do we need to go through geocentric coordinates?
	  if (source.es !== dest.es || source.a !== dest.a || checkParams(fallback) || checkParams(dest.datum_type)) {
	    //DGR: 2012-07-29 : add nadgrids support (end)
	    // Convert to geocentric coordinates.
	    source.geodetic_to_geocentric(point);
	    // CHECK_RETURN;
	    // Convert between datums
	    if (checkParams(source.datum_type)) {
	      source.geocentric_to_wgs84(point);
	      // CHECK_RETURN;
	    }
	    if (checkParams(dest.datum_type)) {
	      dest.geocentric_from_wgs84(point);
	      // CHECK_RETURN;
	    }
	    // Convert back to geodetic coordinates
	    dest.geocentric_to_geodetic(point);
	    // CHECK_RETURN;
	  }
	  // Apply grid shift to destination if required
	  if (dest.datum_type === PJD_GRIDSHIFT) {
	    this.apply_gridshift(dest, 1, point);
	    // CHECK_RETURN;
	  }

	  source.a = src_a;
	  source.es = src_es;
	  dest.a = dst_a;
	  dest.es = dst_es;

	  return point;
	};
	});

	var require$$3$7 = (datum_transform && typeof datum_transform === 'object' && 'default' in datum_transform ? datum_transform['default'] : datum_transform);

	var transform = createCommonjsModule(function (module) {
	var D2R = 0.01745329251994329577;
	var R2D = 57.29577951308232088;
	var PJD_3PARAM = 1;
	var PJD_7PARAM = 2;
	var datum_transform = require$$3$7;
	var adjust_axis = require$$2$5;
	var proj = require$$1$8;
	var toPoint = require$$0$12;
	module.exports = function transform(source, dest, point) {
	  var wgs84;
	  if (Array.isArray(point)) {
	    point = toPoint(point);
	  }
	  function checkNotWGS(source, dest) {
	    return ((source.datum.datum_type === PJD_3PARAM || source.datum.datum_type === PJD_7PARAM) && dest.datumCode !== "WGS84");
	  }

	  // Workaround for datum shifts towgs84, if either source or destination projection is not wgs84
	  if (source.datum && dest.datum && (checkNotWGS(source, dest) || checkNotWGS(dest, source))) {
	    wgs84 = new proj('WGS84');
	    transform(source, wgs84, point);
	    source = wgs84;
	  }
	  // DGR, 2010/11/12
	  if (source.axis !== "enu") {
	    adjust_axis(source, false, point);
	  }
	  // Transform source points to long/lat, if they aren't already.
	  if (source.projName === "longlat") {
	    point.x *= D2R; // convert degrees to radians
	    point.y *= D2R;
	  }
	  else {
	    if (source.to_meter) {
	      point.x *= source.to_meter;
	      point.y *= source.to_meter;
	    }
	    source.inverse(point); // Convert Cartesian to longlat
	  }
	  // Adjust for the prime meridian if necessary
	  if (source.from_greenwich) {
	    point.x += source.from_greenwich;
	  }

	  // Convert datums if needed, and if possible.
	  point = datum_transform(source.datum, dest.datum, point);

	  // Adjust for the prime meridian if necessary
	  if (dest.from_greenwich) {
	    point.x -= dest.from_greenwich;
	  }

	  if (dest.projName === "longlat") {
	    // convert radians to decimal degrees
	    point.x *= R2D;
	    point.y *= R2D;
	  }
	  else { // else project
	    dest.forward(point);
	    if (dest.to_meter) {
	      point.x /= dest.to_meter;
	      point.y /= dest.to_meter;
	    }
	  }

	  // DGR, 2010/11/12
	  if (dest.axis !== "enu") {
	    adjust_axis(dest, true, point);
	  }

	  return point;
	};
	});

	var require$$0$11 = (transform && typeof transform === 'object' && 'default' in transform ? transform['default'] : transform);

	var Point = createCommonjsModule(function (module) {
	var mgrs = require$$0$10;

	function Point(x, y, z) {
	  if (!(this instanceof Point)) {
	    return new Point(x, y, z);
	  }
	  if (Array.isArray(x)) {
	    this.x = x[0];
	    this.y = x[1];
	    this.z = x[2] || 0.0;
	  } else if(typeof x === 'object') {
	    this.x = x.x;
	    this.y = x.y;
	    this.z = x.z || 0.0;
	  } else if (typeof x === 'string' && typeof y === 'undefined') {
	    var coords = x.split(',');
	    this.x = parseFloat(coords[0], 10);
	    this.y = parseFloat(coords[1], 10);
	    this.z = parseFloat(coords[2], 10) || 0.0;
	  } else {
	    this.x = x;
	    this.y = y;
	    this.z = z || 0.0;
	  }
	  console.warn('proj4.Point will be removed in version 3, use proj4.toPoint');
	}

	Point.fromMGRS = function(mgrsStr) {
	  return new Point(mgrs.toPoint(mgrsStr));
	};
	Point.prototype.toMGRS = function(accuracy) {
	  return mgrs.forward([this.x, this.y], accuracy);
	};
	module.exports = Point;
	});

	var require$$6$2 = (Point && typeof Point === 'object' && 'default' in Point ? Point['default'] : Point);

	var core = createCommonjsModule(function (module) {
	var proj = require$$1$8;
	var transform = require$$0$11;
	var wgs84 = proj('WGS84');

	function transformer(from, to, coords) {
	  var transformedArray;
	  if (Array.isArray(coords)) {
	    transformedArray = transform(from, to, coords);
	    if (coords.length === 3) {
	      return [transformedArray.x, transformedArray.y, transformedArray.z];
	    }
	    else {
	      return [transformedArray.x, transformedArray.y];
	    }
	  }
	  else {
	    return transform(from, to, coords);
	  }
	}

	function checkProj(item) {
	  if (item instanceof proj) {
	    return item;
	  }
	  if (item.oProj) {
	    return item.oProj;
	  }
	  return proj(item);
	}
	function proj4(fromProj, toProj, coord) {
	  fromProj = checkProj(fromProj);
	  var single = false;
	  var obj;
	  if (typeof toProj === 'undefined') {
	    toProj = fromProj;
	    fromProj = wgs84;
	    single = true;
	  }
	  else if (typeof toProj.x !== 'undefined' || Array.isArray(toProj)) {
	    coord = toProj;
	    toProj = fromProj;
	    fromProj = wgs84;
	    single = true;
	  }
	  toProj = checkProj(toProj);
	  if (coord) {
	    return transformer(fromProj, toProj, coord);
	  }
	  else {
	    obj = {
	      forward: function(coords) {
	        return transformer(fromProj, toProj, coords);
	      },
	      inverse: function(coords) {
	        return transformer(toProj, fromProj, coords);
	      }
	    };
	    if (single) {
	      obj.oProj = toProj;
	    }
	    return obj;
	  }
	}
	module.exports = proj4;
	});

	var require$$8$1 = (core && typeof core === 'object' && 'default' in core ? core['default'] : core);

	var index$2 = createCommonjsModule(function (module) {
	var proj4 = require$$8$1;
	proj4.defaultDatum = 'WGS84'; //default datum
	proj4.Proj = require$$1$8;
	proj4.WGS84 = new proj4.Proj('WGS84');
	proj4.Point = require$$6$2;
	proj4.toPoint = require$$0$12;
	proj4.defs = require$$2$3;
	proj4.transform = require$$0$11;
	proj4.mgrs = require$$0$10;
	proj4.version = require$$1$7.version;
	require$$0$1(proj4);
	module.exports = proj4;
	});

	var require$$0 = (index$2 && typeof index$2 === 'object' && 'default' in index$2 ? index$2['default'] : index$2);

	(function(self) {
	  'use strict';

	  if (self.fetch) {
	    return
	  }

	  var support = {
	    searchParams: 'URLSearchParams' in self,
	    iterable: 'Symbol' in self && 'iterator' in Symbol,
	    blob: 'FileReader' in self && 'Blob' in self && (function() {
	      try {
	        new Blob()
	        return true
	      } catch(e) {
	        return false
	      }
	    })(),
	    formData: 'FormData' in self,
	    arrayBuffer: 'ArrayBuffer' in self
	  }

	  function normalizeName(name) {
	    if (typeof name !== 'string') {
	      name = String(name)
	    }
	    if (/[^a-z0-9\-#$%&'*+.\^_`|~]/i.test(name)) {
	      throw new TypeError('Invalid character in header field name')
	    }
	    return name.toLowerCase()
	  }

	  function normalizeValue(value) {
	    if (typeof value !== 'string') {
	      value = String(value)
	    }
	    return value
	  }

	  // Build a destructive iterator for the value list
	  function iteratorFor(items) {
	    var iterator = {
	      next: function() {
	        var value = items.shift()
	        return {done: value === undefined, value: value}
	      }
	    }

	    if (support.iterable) {
	      iterator[Symbol.iterator] = function() {
	        return iterator
	      }
	    }

	    return iterator
	  }

	  function Headers(headers) {
	    this.map = {}

	    if (headers instanceof Headers) {
	      headers.forEach(function(value, name) {
	        this.append(name, value)
	      }, this)

	    } else if (headers) {
	      Object.getOwnPropertyNames(headers).forEach(function(name) {
	        this.append(name, headers[name])
	      }, this)
	    }
	  }

	  Headers.prototype.append = function(name, value) {
	    name = normalizeName(name)
	    value = normalizeValue(value)
	    var list = this.map[name]
	    if (!list) {
	      list = []
	      this.map[name] = list
	    }
	    list.push(value)
	  }

	  Headers.prototype['delete'] = function(name) {
	    delete this.map[normalizeName(name)]
	  }

	  Headers.prototype.get = function(name) {
	    var values = this.map[normalizeName(name)]
	    return values ? values[0] : null
	  }

	  Headers.prototype.getAll = function(name) {
	    return this.map[normalizeName(name)] || []
	  }

	  Headers.prototype.has = function(name) {
	    return this.map.hasOwnProperty(normalizeName(name))
	  }

	  Headers.prototype.set = function(name, value) {
	    this.map[normalizeName(name)] = [normalizeValue(value)]
	  }

	  Headers.prototype.forEach = function(callback, thisArg) {
	    Object.getOwnPropertyNames(this.map).forEach(function(name) {
	      this.map[name].forEach(function(value) {
	        callback.call(thisArg, value, name, this)
	      }, this)
	    }, this)
	  }

	  Headers.prototype.keys = function() {
	    var items = []
	    this.forEach(function(value, name) { items.push(name) })
	    return iteratorFor(items)
	  }

	  Headers.prototype.values = function() {
	    var items = []
	    this.forEach(function(value) { items.push(value) })
	    return iteratorFor(items)
	  }

	  Headers.prototype.entries = function() {
	    var items = []
	    this.forEach(function(value, name) { items.push([name, value]) })
	    return iteratorFor(items)
	  }

	  if (support.iterable) {
	    Headers.prototype[Symbol.iterator] = Headers.prototype.entries
	  }

	  function consumed(body) {
	    if (body.bodyUsed) {
	      return Promise.reject(new TypeError('Already read'))
	    }
	    body.bodyUsed = true
	  }

	  function fileReaderReady(reader) {
	    return new Promise(function(resolve, reject) {
	      reader.onload = function() {
	        resolve(reader.result)
	      }
	      reader.onerror = function() {
	        reject(reader.error)
	      }
	    })
	  }

	  function readBlobAsArrayBuffer(blob) {
	    var reader = new FileReader()
	    reader.readAsArrayBuffer(blob)
	    return fileReaderReady(reader)
	  }

	  function readBlobAsText(blob) {
	    var reader = new FileReader()
	    reader.readAsText(blob)
	    return fileReaderReady(reader)
	  }

	  function Body() {
	    this.bodyUsed = false

	    this._initBody = function(body) {
	      this._bodyInit = body
	      if (typeof body === 'string') {
	        this._bodyText = body
	      } else if (support.blob && Blob.prototype.isPrototypeOf(body)) {
	        this._bodyBlob = body
	      } else if (support.formData && FormData.prototype.isPrototypeOf(body)) {
	        this._bodyFormData = body
	      } else if (support.searchParams && URLSearchParams.prototype.isPrototypeOf(body)) {
	        this._bodyText = body.toString()
	      } else if (!body) {
	        this._bodyText = ''
	      } else if (support.arrayBuffer && ArrayBuffer.prototype.isPrototypeOf(body)) {
	        // Only support ArrayBuffers for POST method.
	        // Receiving ArrayBuffers happens via Blobs, instead.
	      } else {
	        throw new Error('unsupported BodyInit type')
	      }

	      if (!this.headers.get('content-type')) {
	        if (typeof body === 'string') {
	          this.headers.set('content-type', 'text/plain;charset=UTF-8')
	        } else if (this._bodyBlob && this._bodyBlob.type) {
	          this.headers.set('content-type', this._bodyBlob.type)
	        } else if (support.searchParams && URLSearchParams.prototype.isPrototypeOf(body)) {
	          this.headers.set('content-type', 'application/x-www-form-urlencoded;charset=UTF-8')
	        }
	      }
	    }

	    if (support.blob) {
	      this.blob = function() {
	        var rejected = consumed(this)
	        if (rejected) {
	          return rejected
	        }

	        if (this._bodyBlob) {
	          return Promise.resolve(this._bodyBlob)
	        } else if (this._bodyFormData) {
	          throw new Error('could not read FormData body as blob')
	        } else {
	          return Promise.resolve(new Blob([this._bodyText]))
	        }
	      }

	      this.arrayBuffer = function() {
	        return this.blob().then(readBlobAsArrayBuffer)
	      }

	      this.text = function() {
	        var rejected = consumed(this)
	        if (rejected) {
	          return rejected
	        }

	        if (this._bodyBlob) {
	          return readBlobAsText(this._bodyBlob)
	        } else if (this._bodyFormData) {
	          throw new Error('could not read FormData body as text')
	        } else {
	          return Promise.resolve(this._bodyText)
	        }
	      }
	    } else {
	      this.text = function() {
	        var rejected = consumed(this)
	        return rejected ? rejected : Promise.resolve(this._bodyText)
	      }
	    }

	    if (support.formData) {
	      this.formData = function() {
	        return this.text().then(decode)
	      }
	    }

	    this.json = function() {
	      return this.text().then(JSON.parse)
	    }

	    return this
	  }

	  // HTTP methods whose capitalization should be normalized
	  var methods = ['DELETE', 'GET', 'HEAD', 'OPTIONS', 'POST', 'PUT']

	  function normalizeMethod(method) {
	    var upcased = method.toUpperCase()
	    return (methods.indexOf(upcased) > -1) ? upcased : method
	  }

	  function Request(input, options) {
	    options = options || {}
	    var body = options.body
	    if (Request.prototype.isPrototypeOf(input)) {
	      if (input.bodyUsed) {
	        throw new TypeError('Already read')
	      }
	      this.url = input.url
	      this.credentials = input.credentials
	      if (!options.headers) {
	        this.headers = new Headers(input.headers)
	      }
	      this.method = input.method
	      this.mode = input.mode
	      if (!body) {
	        body = input._bodyInit
	        input.bodyUsed = true
	      }
	    } else {
	      this.url = input
	    }

	    this.credentials = options.credentials || this.credentials || 'omit'
	    if (options.headers || !this.headers) {
	      this.headers = new Headers(options.headers)
	    }
	    this.method = normalizeMethod(options.method || this.method || 'GET')
	    this.mode = options.mode || this.mode || null
	    this.referrer = null

	    if ((this.method === 'GET' || this.method === 'HEAD') && body) {
	      throw new TypeError('Body not allowed for GET or HEAD requests')
	    }
	    this._initBody(body)
	  }

	  Request.prototype.clone = function() {
	    return new Request(this)
	  }

	  function decode(body) {
	    var form = new FormData()
	    body.trim().split('&').forEach(function(bytes) {
	      if (bytes) {
	        var split = bytes.split('=')
	        var name = split.shift().replace(/\+/g, ' ')
	        var value = split.join('=').replace(/\+/g, ' ')
	        form.append(decodeURIComponent(name), decodeURIComponent(value))
	      }
	    })
	    return form
	  }

	  function headers(xhr) {
	    var head = new Headers()
	    var pairs = (xhr.getAllResponseHeaders() || '').trim().split('\n')
	    pairs.forEach(function(header) {
	      var split = header.trim().split(':')
	      var key = split.shift().trim()
	      var value = split.join(':').trim()
	      head.append(key, value)
	    })
	    return head
	  }

	  Body.call(Request.prototype)

	  function Response(bodyInit, options) {
	    if (!options) {
	      options = {}
	    }

	    this.type = 'default'
	    this.status = options.status
	    this.ok = this.status >= 200 && this.status < 300
	    this.statusText = options.statusText
	    this.headers = options.headers instanceof Headers ? options.headers : new Headers(options.headers)
	    this.url = options.url || ''
	    this._initBody(bodyInit)
	  }

	  Body.call(Response.prototype)

	  Response.prototype.clone = function() {
	    return new Response(this._bodyInit, {
	      status: this.status,
	      statusText: this.statusText,
	      headers: new Headers(this.headers),
	      url: this.url
	    })
	  }

	  Response.error = function() {
	    var response = new Response(null, {status: 0, statusText: ''})
	    response.type = 'error'
	    return response
	  }

	  var redirectStatuses = [301, 302, 303, 307, 308]

	  Response.redirect = function(url, status) {
	    if (redirectStatuses.indexOf(status) === -1) {
	      throw new RangeError('Invalid status code')
	    }

	    return new Response(null, {status: status, headers: {location: url}})
	  }

	  self.Headers = Headers
	  self.Request = Request
	  self.Response = Response

	  self.fetch = function(input, init) {
	    return new Promise(function(resolve, reject) {
	      var request
	      if (Request.prototype.isPrototypeOf(input) && !init) {
	        request = input
	      } else {
	        request = new Request(input, init)
	      }

	      var xhr = new XMLHttpRequest()

	      function responseURL() {
	        if ('responseURL' in xhr) {
	          return xhr.responseURL
	        }

	        // Avoid security warnings on getResponseHeader when not allowed by CORS
	        if (/^X-Request-URL:/m.test(xhr.getAllResponseHeaders())) {
	          return xhr.getResponseHeader('X-Request-URL')
	        }

	        return
	      }

	      xhr.onload = function() {
	        var options = {
	          status: xhr.status,
	          statusText: xhr.statusText,
	          headers: headers(xhr),
	          url: responseURL()
	        }
	        var body = 'response' in xhr ? xhr.response : xhr.responseText
	        resolve(new Response(body, options))
	      }

	      xhr.onerror = function() {
	        reject(new TypeError('Network request failed'))
	      }

	      xhr.ontimeout = function() {
	        reject(new TypeError('Network request failed'))
	      }

	      xhr.open(request.method, request.url, true)

	      if (request.credentials === 'include') {
	        xhr.withCredentials = true
	      }

	      if ('responseType' in xhr && support.blob) {
	        xhr.responseType = 'blob'
	      }

	      request.headers.forEach(function(value, name) {
	        xhr.setRequestHeader(name, value)
	      })

	      xhr.send(typeof request._bodyInit === 'undefined' ? null : request._bodyInit)
	    })
	  }
	  self.fetch.polyfill = true
	})(typeof self !== 'undefined' ? self : this);

	var index = createCommonjsModule(function (module, exports) {
	'use strict';

	Object.defineProperty(exports, "__esModule", {
	  value: true
	});
	exports.get = get;
	exports.load = load;
	exports.set = set;



	var _proj = require$$0;

	var _proj2 = _interopRequireDefault(_proj);

	function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

	function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

	var ROOT_PREFIX = 'http://www.opengis.net/def/crs/';
	var OGC_PREFIX = ROOT_PREFIX + 'OGC/';
	var EPSG_PREFIX = ROOT_PREFIX + 'EPSG/0/';

	/**
	 * @typedef {Object} Projection
	 * @property {function(lonlat: Array<number>): Array<number>} forward
	 *   Transforms a geographic WGS84 [longitude, latitude] coordinate to an [x, y] projection coordinate.
	 * @property {function(xy: Array<number>): Array<number>} inverse
	 *   Transforms an [x, y] projection coordinate to a geographic WGS84 [longitude, latitude] coordinate.
	 */

	// a cache of URI string -> Projection object mappings
	var projCache = {};

	// work-arounds for incorrect epsg.io / proj4 behaviour
	var needsAxesReordering = _defineProperty({}, EPSG_PREFIX + 4326, true);

	// store some built-in projections which are not available on epsg.io
	var LONLAT = (0, _proj2.default)('+proj=longlat +datum=WGS84 +no_defs');
	set(OGC_PREFIX + '1.3/CRS84', LONLAT);
	set(EPSG_PREFIX + 4979, reverseAxes(LONLAT));

	/**
	 * Returns a stored {@link Projection} for a given URI, or {@link undefined} if no {@link Projection} is stored for that URI.
	 * 
	 * @param {string} crsUri The CRS URI for which to return a {@link Projection}.
	 * @return {Projection|undefined} A {@link Projection} object, or {@link undefined} if not stored by {@link load} or {@link set}.
	 * 
	 * @example
	 * // has to be stored previously via load() or set()
	 * var proj = uriproj.get('http://www.opengis.net/def/crs/EPSG/0/27700')
	 * var [longitude, latitude] = [-1.54, 55.5]
	 * var [easting,northing] = proj.forward([longitude, latitude])
	 */
	function get(crsUri) {
	  return projCache[crsUri];
	}

	/**
	 * Returns a {@link Promise} that succeeds with an already stored {@link Projection} or, if not stored,
	 * that remotely loads the {@link Projection} (currently using https://epsg.io), stores it, and then succeeds with it.
	 * 
	 * @param {string} crsUri The CRS URI for which to return a projection.
	 * @return {Promise<Projection,Error>} A {@link Promise} object succeeding with a {@link Projection} object,
	 *   and failing with an {@link Error} object in case of network or PROJ.4 parsing problems.
	 * 
	 * @example <caption>Loading a single projection</caption>
	 * uriproj.load('http://www.opengis.net/def/crs/EPSG/0/27700').then(proj => {
	 *   var [longitude, latitude] = [-1.54, 55.5]
	 *   var [easting,northing] = proj.forward([longitude, latitude])
	 * })
	 * 
	 * @example <caption>Loading multiple projections</caption>
	 * var uris = [
	 *   'http://www.opengis.net/def/crs/EPSG/0/27700',
	 *   'http://www.opengis.net/def/crs/EPSG/0/7376',
	 *   'http://www.opengis.net/def/crs/EPSG/0/7375']
	 * Promise.all(uris.map(uriproj.load)).then(projs => {
	 *   // all projections are loaded and stored now
	 *   
	 *   // get the first projection
	 *   var proj1 = projs[0]
	 *   // or:
	 *   var proj1 = uriproj.get(uris[0])
	 * })
	 * 
	 */
	function load(crsUri) {
	  if (crsUri in projCache) {
	    return Promise.resolve(projCache[crsUri]);
	  }

		var url;
		if (crsUri.indexOf('metadata') > -1) {
			url = crsUri
		}	
		else {
			var epsg = crsUriToEPSG(crsUri);
			url = 'https://epsg.io/' + epsg + '.proj4';
		}



	  return fetch(url).then(function (response) {
	    if (!response.ok) {
	      throw new Error('HTTP response code: ' + response.status);
	    }
	    return response.text();
	  }).then(function (proj4string) {
	    return set(crsUri, proj4string, { reverseAxes: crsUri in needsAxesReordering });
	  });
	}

	/**
	 * Stores a given projection for a given URI that can then be accessed via {@link get} and {@link load}.
	 * 
	 * @param {string} crsUri The CRS URI for which to store the projection.
	 * @param {string|Projection} proj A proj4 string or a {@link Projection} object.
	 * @param {Object} [options] Options object.
	 * @param {boolean} [options.reverseAxes=false] If proj is a proj4 string, whether to reverse the projection axes.
	 * @return {Projection} The newly stored projection.
	 * @throws {Error} If crsUri or proj is missing, or if a PROJ.4 string cannot be parsed by proj4js. 
	 * 
	 * @example <caption>Storing a projection using a PROJ.4 string</caption>
	 * var uri = 'http://www.opengis.net/def/crs/EPSG/0/27700'
	 * var proj4 = '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 ' +
	 *   '+ellps=airy +towgs84=446.448,-125.157,542.06,0.15,0.247,0.842,-20.489 +units=m +no_defs'
	 * uriproj.set(uri, proj4)
	 * 
	 * @example <caption>Storing a projection using a Projection object</caption>
	 * var uri = 'http://www.opengis.net/def/crs/EPSG/0/27700'
	 * var proj = {
	 *   forward: ([lon,lat]) => [..., ...],
	 *   inverse: ([x,y]) => [..., ...]
	 * }
	 * uriproj.set(uri, proj)
	 */
	function set(crsUri, proj) {
	  var options = arguments.length <= 2 || arguments[2] === undefined ? {} : arguments[2];

	  if (!crsUri || !proj) {
	    throw new Error('crsUri and proj cannot be empty');
	  }
	  var projobj = void 0;
	  if (typeof proj === 'string') {
	    projobj = (0, _proj2.default)(proj);
	    if (!projobj) {
	      throw new Error('Unsupported proj4 string: ' + proj);
	    }
	    if (options.reverseAxes) {
	      projobj = reverseAxes(projobj);
	    }
	  } else {
	    projobj = proj;
	  }
	  projCache[crsUri] = projobj;
	  return projobj;
	}

	/**
	 * Return the EPSG code of an OGC CRS URI of the form
	 * http://www.opengis.net/def/crs/EPSG/0/1234 (would return 1234).
	 * 
	 * @param {string} crsUri The CRS URI for which to return the EPSG code.
	 * @return {string} The EPSG code.
	 */
	function crsUriToEPSG(uri) {
	  var epsg = void 0;
	  if (uri.indexOf(EPSG_PREFIX) === 0) {
	    epsg = uri.substr(EPSG_PREFIX.length);
	  } else {
	    throw new Error('Unsupported CRS URI: ' + uri);
	  }

	  return epsg;
	}

	/**
	 * Reverses projection axis order.
	 * 
	 * For example, a projection with lon, lat axis order is turned into one with lat, lon order.
	 * This is necessary since geographic projections in proj4 can only be defined with
	 * lon,lat order, however some CRSs have lat,lon order (like EPSG4326).
	 * Incorrectly, epsg.io returns a proj4 string (with lon,lat order) even if the CRS
	 * has lat,lon order. This function manually flips the axis order of a given projection.
	 * See also `needsAxesReordering` above.
	 * 
	 * @param {Projection} proj The projection whose axis order to revert.
	 * @return {Projection} The projection with reversed axis order.
	 */
	function reverseAxes(proj) {
	  return {
	    forward: function forward(pos) {
	      return proj.forward(pos).reverse();
	    },
	    inverse: function inverse(pos) {
	      return proj.inverse([pos[1], pos[0]]);
	    }
	  };
	}
	});

	var load = index.load;
	var get$1 = index.get;

	var _LongitudeAxisIndex;

	var OPENGIS_CRS_PREFIX = 'http://www.opengis.net/def/crs/';

	/** 3D WGS84 in lat-lon-height order */
	var EPSG4979 = OPENGIS_CRS_PREFIX + 'EPSG/0/4979';

	/** 2D WGS84 in lat-lon order */
	var EPSG4326 = OPENGIS_CRS_PREFIX + 'EPSG/0/4326';

	/** 2D WGS84 in lon-lat order */
	var CRS84 = OPENGIS_CRS_PREFIX + 'OGC/1.3/CRS84';

	/** CRSs in which position is specified by geodetic latitude and longitude */
	var GeographicCRSs = [EPSG4979, EPSG4326, CRS84];

	/** Position of longitude axis */
	var LongitudeAxisIndex = (_LongitudeAxisIndex = {}, defineProperty(_LongitudeAxisIndex, EPSG4979, 1), defineProperty(_LongitudeAxisIndex, EPSG4326, 1), defineProperty(_LongitudeAxisIndex, CRS84, 0), _LongitudeAxisIndex);

	/**
	 * Return the reference system connection object for the given domain coordinate ID,
	 * or undefined if none exists.
	 */
	function getReferenceObject(domain, coordinateId) {
	  var ref = domain.referencing.find(function (ref) {
	    return ref.coordinates.indexOf(coordinateId) !== -1;
	  });
	  return ref;
	}

	/**
	 * Return the reference system connection object of the horizontal CRS of the domain,
	 * or ``undefined`` if none found.
	 * A horizontal CRS is either geodetic (typically ellipsoidal, meaning lat/lon)
	 * or projected, and may be 2D or 3D (including height).
	 */
	function getHorizontalCRSReferenceObject(domain) {
	  var isHorizontal = function isHorizontal(ref) {
	    return ['GeodeticCRS', 'GeographicCRS', 'GeocentricCRS', 'ProjectedCRS'].indexOf(ref.system.type) !== -1;
	  };
	  var ref = domain.referencing.find(isHorizontal);
	  return ref;
	}

	/**
	 * Return whether the reference system is a CRS in which
	 * horizontal position is specified by geodetic latitude and longitude.
	 */
	function isEllipsoidalCRS(rs) {
	  return rs.type === 'GeographicCRS' || GeographicCRSs.indexOf(rs.id) !== -1;
	}

	/**
	 * Return a projection object based on the CRS found in the coverage domain.
	 * If no CRS is found or it is unsupported, then ``undefined`` is returned.
	 * For non-built-in projections, this function returns already-cached projections
	 * that were loaded via {@link loadProjection}.
	 *
	 * A projection converts between geodetic lat/lon and projected x/y values.
	 *
	 * For lat/lon CRSs the projection is defined such that an input lat/lon
	 * position gets projected/wrapped to the longitude range used in the domain, for example
	 * [0,360]. The purpose of this is to make intercomparison between different coverages easier.
	 *
	 * The following limitations currently apply:
	 * - only primitive axes and Tuple/Polygon composite axes are supported for lat/lon CRSs
	 *
	 * @param {Domain} domain A coverage domain object.
	 * @return {IProjection} A stripped-down Leaflet IProjection object.
	 */
	function getProjection(domain) {
	  var isEllipsoidal = domain.referencing.some(function (ref) {
	    return isEllipsoidalCRS(ref.system);
	  });
	  if (isEllipsoidal) {
	    return getLonLatProjection(domain);
	  }

	  // try to get projection via uriproj library
	  var ref = getHorizontalCRSReferenceObject(domain);
	  if (!ref) {
	    throw new Error('No horizontal CRS found in coverage domain');
	  }

	  var uri = ref.system.id;
	  var proj = get$1(uri);
	  if (!proj) {
	    throw new Error('Projection ' + uri + ' not cached in uriproj, use loadProjection() instead');
	  }
	  return wrapProj4(proj);
	}

	/**
	 * Like {@link getProjection} but will also try to remotely load a projection definition via the uriproj library.
	 * On success, the loaded projection is automatically cached for later use and can be directly requested
	 * with {@link getProjection}.
	 *
	 * @param {Domain} domain A coverage domain object.
	 * @return {Promise<IProjection>} A Promise succeeding with a stripped-down Leaflet IProjection object.
	 */
	function loadProjection(domain) {
	  try {
	    // we try the local one first so that we get our special lon/lat projection (which doesn't exist in uriproj)
	    return getProjection(domain);
	  } catch (e) {}

	  // try to load projection remotely via uriproj library
	  var ref = getHorizontalCRSReferenceObject(domain);
	  if (!ref) {
	    throw new Error('No horizontal CRS found in coverage domain');
	  }

	  var uri = ref.system.id;
	  return load(uri).then(function (proj) {
	    return wrapProj4(proj);
	  });
	}

	/**
	 * Return the coordinate IDs of the horizontal CRS of the domain.
	 *
	 * @deprecated use getHorizontalCRSCoordinateIDs
	 * @example
	 * var [xComp,yComp] = getHorizontalCRSComponents(domain)
	 */
	function getHorizontalCRSComponents(domain) {
	  return getHorizontalCRSCoordinateIDs(domain);
	}

	/**
	 * Return the coordinate IDs of the horizontal CRS of the domain.
	 *
	 * @example
	 * var [xComp,yComp] = getHorizontalCRSCoordinateIDs(domain)
	 */
	function getHorizontalCRSCoordinateIDs(domain) {
	  var ref = getHorizontalCRSReferenceObject(domain);
	  return ref.coordinates;
	}

	/**
	 * Wraps a proj4 Projection object into an IProjection object.
	 */
	function wrapProj4(proj) {
	  return {
	    project: function project(_ref) {
	      var lon = _ref.lon;
	      var lat = _ref.lat;

	      var _proj$forward = proj.forward([lon, lat]);

	      var _proj$forward2 = slicedToArray(_proj$forward, 2);

	      var x = _proj$forward2[0];
	      var y = _proj$forward2[1];

	      return { x: x, y: y };
	    },
	    unproject: function unproject(_ref2) {
	      var x = _ref2.x;
	      var y = _ref2.y;

	      var _proj$inverse = proj.inverse([x, y]);

	      var _proj$inverse2 = slicedToArray(_proj$inverse, 2);

	      var lon = _proj$inverse2[0];
	      var lat = _proj$inverse2[1];

	      return { lon: lon, lat: lat };
	    }
	  };
	}

	function getLonLatProjection(domain) {
	  var ref = domain.referencing.find(function (ref) {
	    return isEllipsoidalCRS(ref.system);
	  });
	  var lonIdx = LongitudeAxisIndex[ref.system.id];
	  if (lonIdx > 1) {
	    // this should never happen as longitude is always the first or second axis
	    throw new Error();
	  }

	  var lonComponent = ref.coordinates[lonIdx];

	  // we find the min and max longitude occuring in the domain by inspecting the axis values
	  // Note: this is inefficient for big composite axes.
	  //       In that case, something like a domain extent might help which has the min/max values for each component.
	  // TODO handle bounds
	  var lonMin = void 0,
	      lonMax = void 0;
	  if (domain.axes.has(lonComponent)) {
	    // longitude is a grid axis
	    var lonAxisName = lonComponent;
	    var lonAxisVals = domain.axes.get(lonAxisName).values;
	    lonMin = lonAxisVals[0];
	    lonMax = lonAxisVals[lonAxisVals.length - 1];
	    if (lonMin > lonMax) {
	      var _ref3 = [lonMax, lonMin];
	      lonMin = _ref3[0];
	      lonMax = _ref3[1];
	    }
	  } else {
	    // TODO there should be no dependency to CovJSON

	    // longitude is not a primitive grid axis but a component of a composite axis

	    // find the composite axis containing the longitude component
	    var axes = [].concat(toConsumableArray(domain.axes.values()));
	    var axis = axes.find(function (axis) {
	      return axis.coordinates.indexOf(lonComponent) !== -1;
	    });
	    var lonCompIdx = axis.coordinates.indexOf(lonComponent);

	    // scan the composite axis for min/max longitude values
	    lonMin = Infinity;
	    lonMax = -Infinity;
	    if (axis.dataType === COVJSON_DATATYPE_TUPLE) {
	      var _iteratorNormalCompletion = true;
	      var _didIteratorError = false;
	      var _iteratorError = undefined;

	      try {
	        for (var _iterator = axis.values[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
	          var tuple = _step.value;

	          var lon = tuple[lonCompIdx];
	          lonMin = Math.min(lon, lonMin);
	          lonMax = Math.max(lon, lonMax);
	        }
	      } catch (err) {
	        _didIteratorError = true;
	        _iteratorError = err;
	      } finally {
	        try {
	          if (!_iteratorNormalCompletion && _iterator.return) {
	            _iterator.return();
	          }
	        } finally {
	          if (_didIteratorError) {
	            throw _iteratorError;
	          }
	        }
	      }
	    } else if (axis.dataType === COVJSON_DATATYPE_POLYGON) {
	      var _iteratorNormalCompletion2 = true;
	      var _didIteratorError2 = false;
	      var _iteratorError2 = undefined;

	      try {
	        for (var _iterator2 = axis.values[Symbol.iterator](), _step2; !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
	          var poly = _step2.value;
	          var _iteratorNormalCompletion3 = true;
	          var _didIteratorError3 = false;
	          var _iteratorError3 = undefined;

	          try {
	            for (var _iterator3 = poly[Symbol.iterator](), _step3; !(_iteratorNormalCompletion3 = (_step3 = _iterator3.next()).done); _iteratorNormalCompletion3 = true) {
	              var ring = _step3.value;
	              var _iteratorNormalCompletion4 = true;
	              var _didIteratorError4 = false;
	              var _iteratorError4 = undefined;

	              try {
	                for (var _iterator4 = ring[Symbol.iterator](), _step4; !(_iteratorNormalCompletion4 = (_step4 = _iterator4.next()).done); _iteratorNormalCompletion4 = true) {
	                  var point = _step4.value;

	                  var _lon = point[lonCompIdx];
	                  lonMin = Math.min(_lon, lonMin);
	                  lonMax = Math.max(_lon, lonMax);
	                }
	              } catch (err) {
	                _didIteratorError4 = true;
	                _iteratorError4 = err;
	              } finally {
	                try {
	                  if (!_iteratorNormalCompletion4 && _iterator4.return) {
	                    _iterator4.return();
	                  }
	                } finally {
	                  if (_didIteratorError4) {
	                    throw _iteratorError4;
	                  }
	                }
	              }
	            }
	          } catch (err) {
	            _didIteratorError3 = true;
	            _iteratorError3 = err;
	          } finally {
	            try {
	              if (!_iteratorNormalCompletion3 && _iterator3.return) {
	                _iterator3.return();
	              }
	            } finally {
	              if (_didIteratorError3) {
	                throw _iteratorError3;
	              }
	            }
	          }
	        }
	      } catch (err) {
	        _didIteratorError2 = true;
	        _iteratorError2 = err;
	      } finally {
	        try {
	          if (!_iteratorNormalCompletion2 && _iterator2.return) {
	            _iterator2.return();
	          }
	        } finally {
	          if (_didIteratorError2) {
	            throw _iteratorError2;
	          }
	        }
	      }
	    } else {
	      throw new Error('Unsupported data type: ' + axis.dataType);
	    }
	  }

	  var lonMid = (lonMax + lonMin) / 2;
	  var lonMinExtended = lonMid - 180;
	  var lonMaxExtended = lonMid + 180;

	  return {
	    project: function project(_ref4) {
	      var lon = _ref4.lon;
	      var lat = _ref4.lat;

	      var lonProjected = void 0;
	      if (lonMinExtended <= lon && lon <= lonMaxExtended) {
	        // use unchanged to avoid introducing rounding errors
	        lonProjected = lon;
	      } else {
	        lonProjected = ((lon - lonMinExtended) % 360 + 360) % 360 + lonMinExtended;
	      }

	      var _ref5 = lonIdx === 0 ? [lonProjected, lat] : [lat, lonProjected];

	      var _ref6 = slicedToArray(_ref5, 2);

	      var x = _ref6[0];
	      var y = _ref6[1];

	      return { x: x, y: y };
	    },
	    unproject: function unproject(_ref7) {
	      var x = _ref7.x;
	      var y = _ref7.y;

	      var _ref8 = lonIdx === 0 ? [x, y] : [y, x];

	      var _ref9 = slicedToArray(_ref8, 2);

	      var lon = _ref9[0];
	      var lat = _ref9[1];

	      return { lon: lon, lat: lat };
	    }
	  };
	}

	/**
	 * Reprojects coordinates from one projection to another.
	 */
	function reprojectCoords(pos, fromProjection, toProjection) {
	  return toProjection.project(fromProjection.unproject(pos));
	}

	/**
	 * Returns a function which converts an arbitrary longitude to the
	 * longitude extent used in the coverage domain.
	 * This only supports primitive axes since this is what subsetByValue supports.
	 * The longitude extent is extended to 360 degrees if the actual extent is smaller.
	 * The extension is done equally on both sides of the extent.
	 *
	 * For example, the domain may have longitudes within [0,360].
	 * An input longitude of -70 is converted to 290.
	 * All longitudes within [0,360] are returned unchanged.
	 *
	 * If the domain has longitudes within [10,50] then the
	 * extended longitude range is [-150,210] (-+180 from the middle point).
	 * An input longitude of -170 is converted to 190.
	 * All longitudes within [-150,210] are returned unchanged.
	 *
	 * @ignore
	 */
	function getLongitudeWrapper(domain, axisName) {
	  // TODO deprecate this in favour of getProjection, check leaflet-coverage

	  // for primitive axes, the axis identifier = component identifier
	  if (!isLongitudeAxis(domain, axisName)) {
	    throw new Error('\'' + axisName + '\' is not a longitude axis');
	  }

	  var vals = domain.axes.get(axisName).values;
	  var lon_min = vals[0];
	  var lon_max = vals[vals.length - 1];
	  if (lon_min > lon_max) {
	    var _ref10 = [lon_max, lon_min];
	    lon_min = _ref10[0];
	    lon_max = _ref10[1];
	  }

	  var x_mid = (lon_max + lon_min) / 2;
	  var x_min = x_mid - 180;
	  var x_max = x_mid + 180;

	  return function (lon) {
	    if (x_min <= lon && lon <= x_max) {
	      // directly return to avoid introducing rounding errors
	      return lon;
	    } else {
	      return ((lon - x_min) % 360 + 360) % 360 + x_min;
	    }
	  };
	}

	/**
	 * Return whether the given domain axis represents longitudes.
	 *
	 * @ignore
	 */
	function isLongitudeAxis(domain, axisName) {
	  var ref = getReferenceObject(domain, axisName);
	  if (!ref) {
	    return false;
	  }

	  var crsId = ref.system.id;
	  // TODO should support unknown CRSs with embedded axis information
	  if (GeographicCRSs.indexOf(crsId) === -1) {
	    // this also covers the case when there is no ID property
	    return false;
	  }

	  var compIdx = ref.coordinates.indexOf(axisName);
	  var isLongitude = LongitudeAxisIndex[crsId] === compIdx;
	  return isLongitude;
	}

	/**
	 * Returns true if the given axis has ISO8601 date strings
	 * as axis values.
	 */
	function isISODateAxis(domain, axisName) {
	  var val = domain.axes.get(axisName).values[0];
	  if (typeof val !== 'string') {
	    return false;
	  }
	  return !isNaN(new Date(val).getTime());
	}

	function asTime(inp) {
	  var res = void 0;
	  var err = false;
	  if (typeof inp === 'string') {
	    res = new Date(inp).getTime();
	  } else if (inp instanceof Date) {
	    res = inp.getTime();
	  } else {
	    err = true;
	  }
	  if (isNaN(res)) {
	    err = true;
	  }
	  if (err) {
	    throw new Error('Invalid date: ' + inp);
	  }
	  return res;
	}

	/**
	 * After normalization, all constraints are start,stop,step objects.
	 * It holds that stop > start, step > 0, start >= 0, stop >= 1.
	 * For each axis, a constraint exists.
	 */
	function normalizeIndexSubsetConstraints(domain, constraints) {
	  // check and normalize constraints to simplify code
	  var normalizedConstraints = {};
	  for (var axisName in constraints) {
	    if (!domain.axes.has(axisName)) {
	      // TODO clarify cov behaviour in the JS API spec
	      continue;
	    }
	    if (constraints[axisName] === undefined || constraints[axisName] === null) {
	      continue;
	    }
	    if (typeof constraints[axisName] === 'number') {
	      var constraint = constraints[axisName];
	      normalizedConstraints[axisName] = { start: constraint, stop: constraint + 1 };
	    } else {
	      normalizedConstraints[axisName] = constraints[axisName];
	    }

	    var _normalizedConstraint = normalizedConstraints[axisName];
	    var _normalizedConstraint2 = _normalizedConstraint.start;
	    var start = _normalizedConstraint2 === undefined ? 0 : _normalizedConstraint2;
	    var _normalizedConstraint3 = _normalizedConstraint.stop;
	    var stop = _normalizedConstraint3 === undefined ? domain.axes.get(axisName).values.length : _normalizedConstraint3;
	    var _normalizedConstraint4 = _normalizedConstraint.step;
	    var step = _normalizedConstraint4 === undefined ? 1 : _normalizedConstraint4;

	    if (step <= 0) {
	      throw new Error('Invalid constraint for ' + axisName + ': step=' + step + ' must be > 0');
	    }
	    if (start >= stop || start < 0) {
	      throw new Error('Invalid constraint for ' + axisName + ': stop=' + stop + ' must be > start=' + start + ' and both >= 0');
	    }
	    normalizedConstraints[axisName] = { start: start, stop: stop, step: step };
	  }
	  var _iteratorNormalCompletion = true;
	  var _didIteratorError = false;
	  var _iteratorError = undefined;

	  try {
	    for (var _iterator = domain.axes.keys()[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
	      var _axisName = _step.value;

	      if (!(_axisName in normalizedConstraints)) {
	        var len = domain.axes.get(_axisName).values.length;
	        normalizedConstraints[_axisName] = { start: 0, stop: len, step: 1 };
	      }
	    }
	  } catch (err) {
	    _didIteratorError = true;
	    _iteratorError = err;
	  } finally {
	    try {
	      if (!_iteratorNormalCompletion && _iterator.return) {
	        _iterator.return();
	      }
	    } finally {
	      if (_didIteratorError) {
	        throw _iteratorError;
	      }
	    }
	  }

	  return normalizedConstraints;
	}

	function subsetDomainByIndex(domain, constraints) {
	  constraints = normalizeIndexSubsetConstraints(domain, constraints);

	  // subset the axis arrays of the domain (immediately + cached)
	  var newdomain = {
	    type: DOMAIN,
	    domainType: domain.domainType,
	    axes: new Map(domain.axes),
	    referencing: domain.referencing
	  };

	  var _iteratorNormalCompletion2 = true;
	  var _didIteratorError2 = false;
	  var _iteratorError2 = undefined;

	  try {
	    var _loop = function _loop() {
	      var axisName = _step2.value;

	      var axis = domain.axes.get(axisName);
	      var coords = axis.values;
	      var bounds = axis.bounds;
	      var constraint = constraints[axisName];
	      var newcoords = void 0;
	      var newbounds = void 0;

	      var start = constraint.start;
	      var stop = constraint.stop;
	      var step = constraint.step;

	      if (start === 0 && stop === coords.length && step === 1) {
	        newcoords = coords;
	        newbounds = bounds;
	      } else if (step === 1) {
	        // TypedArray has subarray which creates a view, while Array has slice which makes a copy
	        if (coords.subarray) {
	          newcoords = coords.subarray(start, stop);
	        } else {
	          newcoords = coords.slice(start, stop);
	        }
	        if (bounds) {
	          newbounds = {
	            get: function get(i) {
	              return bounds.get(start + i);
	            }
	          };
	        }
	      } else {
	        var q = Math.trunc((stop - start) / step);
	        var r = (stop - start) % step;
	        var len = q + r;
	        newcoords = new coords.constructor(len); // array or typed array
	        for (var i = start, j = 0; i < stop; i += step, j++) {
	          newcoords[j] = coords[i];
	        }
	        if (bounds) {
	          newbounds = {
	            get: function get(i) {
	              return bounds.get(start + i * step);
	            }
	          };
	        }
	      }

	      var newaxis = {
	        dataType: axis.dataType,
	        coordinates: axis.coordinates,
	        values: newcoords,
	        bounds: newbounds
	      };
	      newdomain.axes.set(axisName, newaxis);
	    };

	    for (var _iterator2 = Object.keys(constraints)[Symbol.iterator](), _step2; !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
	      _loop();
	    }
	  } catch (err) {
	    _didIteratorError2 = true;
	    _iteratorError2 = err;
	  } finally {
	    try {
	      if (!_iteratorNormalCompletion2 && _iterator2.return) {
	        _iterator2.return();
	      }
	    } finally {
	      if (_didIteratorError2) {
	        throw _iteratorError2;
	      }
	    }
	  }

	  return newdomain;
	}

	/**
	 * Returns a copy of the grid coverage subsetted to the given bounding box.
	 *
	 * Any grid cell is included which intersects with the bounding box.
	 *
	 * @param {Coverage} cov A Coverage object with domain Grid.
	 * @param {array} bbox [xmin,ymin,xmax,ymax] in native CRS coordinates.
	 * @param {array} [axes=['x','y']] Axis names [x,y].
	 * @returns {Promise<Coverage>} A promise with a Coverage object as result.
	 */
	function subsetByBbox(cov, bbox) {
	  var _cov$subsetByValue;

	  var axes = arguments.length <= 2 || arguments[2] === undefined ? ['x', 'y'] : arguments[2];

	  var _bbox = slicedToArray(bbox, 4);

	  var xmin = _bbox[0];
	  var ymin = _bbox[1];
	  var xmax = _bbox[2];
	  var ymax = _bbox[3];

	  return cov.subsetByValue((_cov$subsetByValue = {}, defineProperty(_cov$subsetByValue, axes[0], { start: xmin, stop: xmax }), defineProperty(_cov$subsetByValue, axes[1], { start: ymin, stop: ymax }), _cov$subsetByValue));
	}

	/**
	 * Generic subsetByIndex function that can be used when building new Coverage objects.
	 *
	 * @example
	 * var cov = {
	 *   type: 'Coverage',
	 *   ...
	 *   subsetByIndex: constraints => CovUtils.subsetByIndex(cov, constraints)
	 * }
	 */
	function subsetByIndex(cov, constraints) {
	  return cov.loadDomain().then(function (domain) {
	    constraints = normalizeIndexSubsetConstraints(domain, constraints);
	    var newdomain = subsetDomainByIndex(domain, constraints);

	    // subset ranges (on request)
	    var rangeWrapper = function rangeWrapper(range) {
	      var newrange = {
	        dataType: range.dataType,
	        get: function get(obj) {
	          // translate subsetted to original indices
	          var newobj = {};
	          var _iteratorNormalCompletion = true;
	          var _didIteratorError = false;
	          var _iteratorError = undefined;

	          try {
	            for (var _iterator = Object.keys(obj)[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
	              var axisName = _step.value;
	              var _constraints$axisName = constraints[axisName];
	              var start = _constraints$axisName.start;
	              var step = _constraints$axisName.step;

	              newobj[axisName] = start + obj[axisName] * step;
	            }
	          } catch (err) {
	            _didIteratorError = true;
	            _iteratorError = err;
	          } finally {
	            try {
	              if (!_iteratorNormalCompletion && _iterator.return) {
	                _iterator.return();
	              }
	            } finally {
	              if (_didIteratorError) {
	                throw _iteratorError;
	              }
	            }
	          }

	          return range.get(newobj);
	        }
	      };
	      newrange.shape = new Map();
	      var _iteratorNormalCompletion2 = true;
	      var _didIteratorError2 = false;
	      var _iteratorError2 = undefined;

	      try {
	        for (var _iterator2 = domain.axes.keys()[Symbol.iterator](), _step2; !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
	          var axisName = _step2.value;

	          var size = newdomain.axes.get(axisName).values.length;
	          newrange.shape.set(axisName, size);
	        }
	      } catch (err) {
	        _didIteratorError2 = true;
	        _iteratorError2 = err;
	      } finally {
	        try {
	          if (!_iteratorNormalCompletion2 && _iterator2.return) {
	            _iterator2.return();
	          }
	        } finally {
	          if (_didIteratorError2) {
	            throw _iteratorError2;
	          }
	        }
	      }

	      return newrange;
	    };

	    var loadRange = function loadRange(key) {
	      return cov.loadRange(key).then(rangeWrapper);
	    };

	    var loadRanges = function loadRanges(keys) {
	      return cov.loadRanges(keys).then(function (ranges) {
	        return new Map([].concat(toConsumableArray(ranges)).map(function (_ref) {
	          var _ref2 = slicedToArray(_ref, 2);

	          var key = _ref2[0];
	          var range = _ref2[1];
	          return [key, rangeWrapper(range)];
	        }));
	      });
	    };

	    // assemble everything to a new coverage
	    var newcov = {
	      type: COVERAGE,
	      domainType: cov.domainType,
	      parameters: cov.parameters,
	      loadDomain: function loadDomain() {
	        return Promise.resolve(newdomain);
	      },
	      loadRange: loadRange,
	      loadRanges: loadRanges
	    };
	    newcov.subsetByIndex = subsetByIndex.bind(null, newcov);
	    newcov.subsetByValue = subsetByValue.bind(null, newcov);
	    return newcov;
	  });
	}

	/**
	 * Generic subsetByValue function that can be used when building new Coverage objects.
	 * Requires cov.subsetByIndex function.
	 *
	 * @example
	 * var cov = {
	 *   type: 'Coverage',
	 *   ...
	 *   subsetByValue: constraints => CovUtils.subsetByValue(cov, constraints)
	 * }
	 */
	function subsetByValue(cov, constraints) {
	  return cov.loadDomain().then(function (domain) {
	    // calculate indices and use subsetByIndex
	    var indexConstraints = {};

	    var _iteratorNormalCompletion3 = true;
	    var _didIteratorError3 = false;
	    var _iteratorError3 = undefined;

	    try {
	      for (var _iterator3 = Object.keys(constraints)[Symbol.iterator](), _step3; !(_iteratorNormalCompletion3 = (_step3 = _iterator3.next()).done); _iteratorNormalCompletion3 = true) {
	        var axisName = _step3.value;

	        var spec = constraints[axisName];
	        if (spec === undefined || spec === null || !domain.axes.has(axisName)) {
	          continue;
	        }
	        var axis = domain.axes.get(axisName);
	        var vals = axis.values;

	        // special-case handling
	        var isISODate = isISODateAxis(domain, axisName);
	        var isLongitude = isLongitudeAxis(domain, axisName);

	        // wrap input longitudes into longitude range of domain axis
	        var lonWrapper = isLongitude ? getLongitudeWrapper(domain, axisName) : undefined;

	        if (typeof spec === 'number' || typeof spec === 'string' || spec instanceof Date) {
	          var match = spec;
	          if (isISODate) {
	            // convert times to numbers before searching
	            match = asTime(match);
	            vals = vals.map(function (v) {
	              return new Date(v).getTime();
	            });
	          } else if (isLongitude) {
	            match = lonWrapper(match);
	          }
	          var i = void 0;
	          // older browsers don't have TypedArray.prototype.indexOf
	          if (vals.indexOf) {
	            i = vals.indexOf(match);
	          } else {
	            i = Array.prototype.indexOf.call(vals, match);
	          }
	          if (i === -1) {
	            throw new Error('Domain value not found: ' + spec);
	          }
	          indexConstraints[axisName] = i;
	        } else if ('target' in spec) {
	          // find index of value closest to target
	          var target = spec.target;
	          if (isISODate) {
	            // convert times to numbers before searching
	            target = asTime(target);
	            vals = vals.map(function (v) {
	              return new Date(v).getTime();
	            });
	          } else if (isLongitude) {
	            target = lonWrapper(target);
	          } else if (typeof vals[0] !== 'number' || typeof target !== 'number') {
	            throw new Error('Invalid axis or constraint value type');
	          }
	          var _i = indexOfNearest(vals, target);
	          indexConstraints[axisName] = _i;
	        } else if ('start' in spec && 'stop' in spec) {
	          // TODO what about bounds?

	          var start = spec.start;
	          var stop = spec.stop;

	          if (isISODate) {
	            var _ref3 = [asTime(start), asTime(stop)];
	            // convert times to numbers before searching

	            start = _ref3[0];
	            stop = _ref3[1];

	            vals = vals.map(function (v) {
	              return new Date(v).getTime();
	            });
	          } else if (isLongitude) {
	            var _ref4 = [lonWrapper(start), lonWrapper(stop)];
	            start = _ref4[0];
	            stop = _ref4[1];
	          } else if (typeof vals[0] !== 'number' || typeof start !== 'number') {
	            throw new Error('Invalid axis or constraint value type');
	          }

	          var _indicesOfNearest = indicesOfNearest(vals, start);

	          var _indicesOfNearest2 = slicedToArray(_indicesOfNearest, 2);

	          var lo1 = _indicesOfNearest2[0];
	          var hi1 = _indicesOfNearest2[1];

	          var _indicesOfNearest3 = indicesOfNearest(vals, stop);

	          var _indicesOfNearest4 = slicedToArray(_indicesOfNearest3, 2);

	          var lo2 = _indicesOfNearest4[0];
	          var hi2 = _indicesOfNearest4[1];

	          // cov is a bit arbitrary and may include one or two indices too much
	          // (but since we don't handle bounds it doesn't matter that much)

	          var imin = Math.min(lo1, hi1, lo2, hi2);
	          var imax = Math.max(lo1, hi1, lo2, hi2) + 1; // subsetByIndex is exclusive

	          indexConstraints[axisName] = { start: imin, stop: imax };
	        } else {
	          throw new Error('Invalid subset constraints');
	        }
	      }
	    } catch (err) {
	      _didIteratorError3 = true;
	      _iteratorError3 = err;
	    } finally {
	      try {
	        if (!_iteratorNormalCompletion3 && _iterator3.return) {
	          _iterator3.return();
	        }
	      } finally {
	        if (_didIteratorError3) {
	          throw _iteratorError3;
	        }
	      }
	    }

	    return cov.subsetByIndex(indexConstraints);
	  });
	}

	/**
	 * Wraps a Domain into a Coverage object by adding dummy parameter and range data.
	 *
	 * @param {Domain} domain the Domain object
	 * @param {array} [options.gridAxes] The horizontal grid axis names, used for checkerboard pattern.
	 * @return {Coverage}
	 */
	function fromDomain(domain) {
	  var options = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];

	  checkDomain(domain);

	  var _options$gridAxes = options.gridAxes;
	  _options$gridAxes = _options$gridAxes === undefined ? ['x', 'y'] : _options$gridAxes;

	  var _options$gridAxes2 = slicedToArray(_options$gridAxes, 2);

	  var x = _options$gridAxes2[0];
	  var y = _options$gridAxes2[1];


	  var dummyKey = 'domain';
	  var dummyLabel = 'Domain';

	  var assumeGrid = domain.axes.has(x) && domain.axes.has(y) && (domain.axes.get(x).values.length > 1 || domain.axes.get(y).values.length > 1);
	  var categories = void 0;
	  var categoryEncoding = void 0;
	  var a = 'a';
	  var av = 0;
	  var b = 'b';
	  var bv = 1;
	  if (assumeGrid) {
	    categories = [{
	      id: a,
	      label: { en: 'A' }
	    }, {
	      id: b,
	      label: { en: 'B' }
	    }];
	    categoryEncoding = new Map([[a, [av]], [b, [bv]]]);
	  } else {
	    categories = [{
	      id: a,
	      label: { en: 'X' }
	    }];
	    categoryEncoding = new Map([[a, [av]]]);
	  }

	  var parameters = new Map();
	  parameters.set(dummyKey, {
	    key: dummyKey,
	    observedProperty: {
	      label: { en: dummyLabel },
	      categories: categories
	    },
	    categoryEncoding: categoryEncoding
	  });

	  var shape = new Map([].concat(toConsumableArray(domain.axes)).map(function (_ref) {
	    var _ref2 = slicedToArray(_ref, 2);

	    var name = _ref2[0];
	    var axis = _ref2[1];
	    return [name, axis.values.length];
	  }));

	  var get = void 0;
	  if (assumeGrid) {
	    (function () {
	      // checkerboard pattern to see grid cells
	      var isOdd = function isOdd(n) {
	        return n % 2;
	      };
	      get = function get(_ref3) {
	        var _ref3$x = _ref3.x;
	        var x = _ref3$x === undefined ? 0 : _ref3$x;
	        var _ref3$y = _ref3.y;
	        var y = _ref3$y === undefined ? 0 : _ref3$y;
	        return isOdd(x + y) ? av : bv;
	      };
	    })();
	  } else {
	    get = function get() {
	      return av;
	    };
	  }

	  var loadRange = function loadRange() {
	    return Promise.resolve({
	      shape: shape,
	      dataType: 'integer',
	      get: get
	    });
	  };

	  var cov = {
	    type: COVERAGE,
	    domainType: domain.domainType,
	    parameters: parameters,
	    loadDomain: function loadDomain() {
	      return Promise.resolve(domain);
	    },
	    loadRange: loadRange
	  };
	  addLoadRangesFunction(cov);
	  addSubsetFunctions(cov);
	  return cov;
	}

	/**
	 * Creates a Coverage with a single parameter from an xndarray object.
	 *
	 * @example
	 * var arr = xndarray(new Float64Array(
	 *   [ 1,2,3,
	 *     4,5,6 ]), {
	 *   shape: [2,3],
	 *   names: ['y','x'],
	 *   coords: {
	 *     y: [10,12,14],
	 *     x: [100,101,102],
	 *     t: [new Date('2001-01-01')]
	 *   }
	 * })
	 * var cov = CovUtils.fromXndarray(arr, {
	 *   parameter: {
	 *     key: 'temperature',
	 *     observedProperty: {
	 *       label: {en: 'Air temperature'}
	 *     },
	 *     unit: { symbol: '°C' }
	 *   }
	 * })
	 * let param = cov.parameters.get('temperature')
	 * let unit = param.unit.symbol // °C
	 * cov.loadRange('temperature').then(temps => {
	 *   let val = temps.get({x:0, y:1}) // val == 4
	 * })
	 *
	 * @param {xndarray} xndarr - Coordinates must be primitive, not tuples etc.
	 * @param {object} [options] Options object.
	 * @param {Parameter} [options.parameter] Specifies the parameter, default parameter has a key of 'p1'.
	 * @param {string} [options.domainType] A domain type URI.
	 * @param {Array<object>} [options.referencing] Optional referencing system info,
	 *   defaults to longitude/latitude in WGS84 for x/y axes and ISO8601 time strings for t axis.
	 * @return {Coverage}
	 */
	function fromXndarray(xndarr) {
	  var options = arguments.length <= 1 || arguments[1] === undefined ? {} : arguments[1];
	  var _options$parameter = options.parameter;
	  var parameter = _options$parameter === undefined ? {
	    key: 'p1',
	    observedProperty: {
	      label: { en: 'Parameter 1' }
	    }
	  } : _options$parameter;
	  var referencing = options.referencing;
	  var domainType = options.domainType;


	  var parameters = new Map();
	  parameters.set(parameter.key, parameter);

	  // assume lon/lat/ISO time for x/y/t by default, for convenience
	  if (!referencing) {
	    referencing = [];
	    if (xndarr.coords.has('x') && xndarr.coords.has('y')) {
	      referencing.push({
	        coordinates: ['x', 'y'],
	        system: {
	          type: 'GeographicCRS',
	          id: 'http://www.opengis.net/def/crs/OGC/1.3/CRS84'
	        }
	      });
	    }
	    if (xndarr.coords.has('t')) {
	      referencing.push({
	        coordinates: ['t'],
	        system: {
	          type: 'TemporalRS',
	          calendar: 'Gregorian'
	        }
	      });
	    }
	  }

	  var axes = new Map();
	  var _iteratorNormalCompletion = true;
	  var _didIteratorError = false;
	  var _iteratorError = undefined;

	  try {
	    for (var _iterator = xndarr.coords[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
	      var _step$value = slicedToArray(_step.value, 2);

	      var axisName = _step$value[0];
	      var vals1Dnd = _step$value[1];

	      var values = new Array(vals1Dnd.size);
	      for (var i = 0; i < vals1Dnd.size; i++) {
	        values[i] = vals1Dnd.get(i);
	      }
	      axes.set(axisName, {
	        key: axisName,
	        coordinates: [axisName],
	        values: values
	      });
	    }
	  } catch (err) {
	    _didIteratorError = true;
	    _iteratorError = err;
	  } finally {
	    try {
	      if (!_iteratorNormalCompletion && _iterator.return) {
	        _iterator.return();
	      }
	    } finally {
	      if (_didIteratorError) {
	        throw _iteratorError;
	      }
	    }
	  }

	  var domain = {
	    type: DOMAIN,
	    domainType: domainType,
	    referencing: referencing,
	    axes: axes
	  };

	  var shape = new Map([].concat(toConsumableArray(domain.axes)).map(function (_ref4) {
	    var _ref5 = slicedToArray(_ref4, 2);

	    var name = _ref5[0];
	    var axis = _ref5[1];
	    return [name, axis.values.length];
	  }));
	  var dataType = xndarr.dtype.indexOf('int') !== -1 ? 'integer' : 'float';

	  var loadRange = function loadRange() {
	    return Promise.resolve({
	      shape: shape,
	      dataType: dataType,
	      get: xndarr.xget.bind(xndarr)
	    });
	  };

	  var cov = {
	    type: COVERAGE,
	    domainType: domainType,
	    parameters: parameters,
	    loadDomain: function loadDomain() {
	      return Promise.resolve(domain);
	    },
	    loadRange: loadRange
	  };
	  addLoadRangesFunction(cov);
	  addSubsetFunctions(cov);
	  return cov;
	}

	function addSubsetFunctions(cov) {
	  checkCoverage(cov);
	  cov.subsetByIndex = subsetByIndex.bind(null, cov);
	  cov.subsetByValue = subsetByValue.bind(null, cov);
	}

	function addLoadRangesFunction(cov) {
	  checkCoverage(cov);
	  function loadRanges(keys) {
	    if (!keys) {
	      keys = cov.parameters.keys();
	    }
	    return Promise.all([].concat(toConsumableArray(keys)).map(cov.loadRange)).then(function (ranges) {
	      return new Map(keys.map(function (key, i) {
	        return [key, ranges[i]];
	      }));
	    });
	  }
	  cov.loadRanges = loadRanges;
	}

	/**
	 * Shallow clone a given object.
	 *
	 * Note: This does *not* handle all kinds of objects!
	 *
	 * @ignore
	 */
	function shallowcopy(obj) {
	  var copy = void 0;
	  if (obj instanceof Map) {
	    copy = new Map(obj);
	  } else {
	    copy = Object.create(Object.getPrototypeOf(obj));
	    for (var prop in obj) {
	      copy[prop] = obj[prop];
	    }
	  }
	  return copy;
	}

	/**
	 * Reproject a coverage.
	 *
	 * Reprojecting means returning a new coverage where the horizontal CRS is replaced
	 * and the horizontal domain coordinates are reprojected.
	 *
	 * Current limitations:
	 * - only point-type coverage domains are supported (Tuple only)
	 * - only horizontal CRSs (2-dimensional) are supported
	 * - non-lat/lon CRSs have to be pre-cached with loadProjection()
	 *
	 * @param {Coverage} cov The Coverage object to reproject.
	 * @param {Domain} refDomain The reference domain from which the horizontal CRS is used.
	 * @returns {Promise<Coverage>} A promise with the reprojected Coverage object as result.
	 */
	function reproject(cov, refDomain) {
	  return cov.loadDomain().then(function (sourceDomain) {
	    var sourceRef = getHorizontalCRSReferenceObject(sourceDomain);
	    if (sourceRef.coordinates.length > 2) {
	      throw new Error('Reprojection not supported for >2D CRSs');
	    }
	    // check that the CRS coordinate IDs don't refer to grid axes
	    if (sourceRef.coordinates.some(sourceDomain.axes.has)) {
	      throw new Error('Grid reprojection not supported yet');
	    }

	    var _sourceRef$coordinate = slicedToArray(sourceRef.coordinates, 2);

	    var xComp = _sourceRef$coordinate[0];
	    var yComp = _sourceRef$coordinate[1];

	    // TODO reproject bounds

	    // find the composite axis that contains the horizontal coordinates

	    var axes = [].concat(toConsumableArray(sourceDomain.axes.values()));
	    var axis = axes.find(function (axis) {
	      return sourceRef.coordinates.every(function (comp) {
	        return axis.coordinates.indexOf(comp) !== -1;
	      });
	    });
	    var xCompIdx = axis.coordinates.indexOf(xComp);
	    var yCompIdx = axis.coordinates.indexOf(yComp);

	    // find the target CRS and get the projection

	    var sourceProjection = getProjection(sourceDomain);
	    var targetProjection = getProjection(refDomain);

	    // reproject the x/y part of every axis value
	    // this is done by unprojecting to lon/lat, followed by projecting to the target x/y
	    var values = void 0;
	    if (axis.dataType === COVJSON_DATATYPE_TUPLE) {
	      // make a deep copy of the axis values and replace x,y values by the reprojected ones
	      values = axis.values.map(function (tuple) {
	        return tuple.slice();
	      });
	      var _iteratorNormalCompletion = true;
	      var _didIteratorError = false;
	      var _iteratorError = undefined;

	      try {
	        for (var _iterator = values[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
	          var tuple = _step.value;
	          var sourceX = tuple[xCompIdx];
	          var sourceY = tuple[yCompIdx];

	          var latlon = sourceProjection.unproject({ x: sourceX, y: sourceY });

	          var _targetProjection$pro = targetProjection.project(latlon);

	          var x = _targetProjection$pro.x;
	          var y = _targetProjection$pro.y;

	          tuple[xCompIdx] = x;
	          tuple[yCompIdx] = y;
	        }
	      } catch (err) {
	        _didIteratorError = true;
	        _iteratorError = err;
	      } finally {
	        try {
	          if (!_iteratorNormalCompletion && _iterator.return) {
	            _iterator.return();
	          }
	        } finally {
	          if (_didIteratorError) {
	            throw _iteratorError;
	          }
	        }
	      }
	    } else {
	      throw new Error('Unsupported data type: ' + axis.dataType);
	    }

	    // assemble reprojected coverage
	    var newAxes = new Map(sourceDomain.axes);
	    var newAxis = shallowcopy(axis);
	    delete newAxis.bounds;
	    newAxis.values = values;
	    newAxes.set(axis.key, newAxis);

	    var targetRef = getHorizontalCRSReferenceObject(refDomain);
	    if (targetRef.coordinates.length > 2) {
	      throw new Error('Reprojection not supported for >2D CRSs');
	    }
	    var newReferencing = sourceDomain.referencing.map(function (ref) {
	      if (ref === sourceRef) {
	        return {
	          coordinates: sourceRef.coordinates,
	          system: targetRef.system
	        };
	      } else {
	        return ref;
	      }
	    });

	    var newDomain = {
	      type: DOMAIN,
	      domainType: sourceDomain.domainType,
	      axes: newAxes,
	      referencing: newReferencing
	    };

	    var newCoverage = {
	      type: COVERAGE,
	      domainType: cov.domainType,
	      parameters: cov.parameters,
	      loadDomain: function loadDomain() {
	        return Promise.resolve(newDomain);
	      },
	      loadRange: function loadRange(paramKey) {
	        return cov.loadRange(paramKey);
	      },
	      loadRanges: function loadRanges(paramKeys) {
	        return cov.loadRanges(paramKeys);
	      },
	      subsetByIndex: function subsetByIndex(constraints) {
	        return cov.subsetByIndex(constraints).then(function (sub) {
	          return reproject(sub, refDomain);
	        });
	      },
	      subsetByValue: function subsetByValue(constraints) {
	        return cov.subsetByValue(constraints).then(function (sub) {
	          return reproject(sub, refDomain);
	        });
	      }
	    };
	    return newCoverage;
	  });
	}

	/**
	 * Returns a copy of the given Coverage object with the parameters
	 * replaced by the supplied ones.
	 *
	 * Note that this is a low-level function and no checks are done on the supplied parameters.
	 */
	function withParameters(cov, params) {
	  var newcov = {
	    type: COVERAGE,
	    domainType: cov.domainType,
	    parameters: params,
	    loadDomain: function loadDomain() {
	      return cov.loadDomain();
	    },
	    loadRange: function loadRange(key) {
	      return cov.loadRange(key);
	    },
	    loadRanges: function loadRanges(keys) {
	      return cov.loadRanges(keys);
	    },
	    subsetByIndex: function subsetByIndex(constraints) {
	      return cov.subsetByIndex(constraints).then(function (sub) {
	        return withParameters(sub, params);
	      });
	    },
	    subsetByValue: function subsetByValue(constraints) {
	      return cov.subsetByValue(constraints).then(function (sub) {
	        return withParameters(sub, params);
	      });
	    }
	  };
	  return newcov;
	}

	/**
	 * Returns a copy of the given Coverage object with the categories
	 * of a given parameter replaced by the supplied ones and the encoding
	 * adapted to the given mapping from old to new.
	 *
	 * @param {Coverage} cov The Coverage object.
	 * @param {String} key The key of the parameter to work with.
	 * @param {object} observedProperty The new observed property including the new array of category objects
	 *                           that will be part of the returned coverage.
	 * @param {Map<String,String>} mapping A mapping from source category id to destination category id.
	 * @returns {Coverage}
	 */
	function withCategories(cov, key, observedProperty, mapping) {
	  /* check breaks with Babel, see https://github.com/jspm/jspm-cli/issues/1348
	  if (!(mapping instanceof Map)) {
	    throw new Error('mapping parameter must be a Map from/to category ID')
	  }
	  */
	  checkCoverage(cov);
	  if (observedProperty.categories.some(function (c) {
	    return !c.id;
	  })) {
	    throw new Error('At least one category object is missing the "id" property');
	  }
	  var newparams = shallowcopy(cov.parameters);
	  var newparam = shallowcopy(newparams.get(key));
	  newparams.set(key, newparam);
	  newparams.get(key).observedProperty = observedProperty;

	  var fromCatEnc = cov.parameters.get(key).categoryEncoding;
	  var catEncoding = new Map();
	  var categories = observedProperty.categories;
	  var _iteratorNormalCompletion = true;
	  var _didIteratorError = false;
	  var _iteratorError = undefined;

	  try {
	    for (var _iterator = categories[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
	      var category = _step.value;

	      var vals = [];
	      var _iteratorNormalCompletion2 = true;
	      var _didIteratorError2 = false;
	      var _iteratorError2 = undefined;

	      try {
	        for (var _iterator2 = mapping[Symbol.iterator](), _step2; !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
	          var _step2$value = slicedToArray(_step2.value, 2);

	          var fromCatId = _step2$value[0];
	          var toCatId = _step2$value[1];

	          if (toCatId === category.id && fromCatEnc.has(fromCatId)) {
	            vals.push.apply(vals, toConsumableArray(fromCatEnc.get(fromCatId)));
	          }
	        }
	      } catch (err) {
	        _didIteratorError2 = true;
	        _iteratorError2 = err;
	      } finally {
	        try {
	          if (!_iteratorNormalCompletion2 && _iterator2.return) {
	            _iterator2.return();
	          }
	        } finally {
	          if (_didIteratorError2) {
	            throw _iteratorError2;
	          }
	        }
	      }

	      if (vals.length > 0) {
	        catEncoding.set(category.id, vals);
	      }
	    }
	  } catch (err) {
	    _didIteratorError = true;
	    _iteratorError = err;
	  } finally {
	    try {
	      if (!_iteratorNormalCompletion && _iterator.return) {
	        _iterator.return();
	      }
	    } finally {
	      if (_didIteratorError) {
	        throw _iteratorError;
	      }
	    }
	  }

	  newparams.get(key).categoryEncoding = catEncoding;

	  var newcov = withParameters(cov, newparams);
	  return newcov;
	}

	/**
	 * Returns a new coverage where the domainType field of the coverage and the domain
	 * is set to the given one.
	 *
	 * @param {Coverage} cov The Coverage object.
	 * @param {String} domainType The new domain type.
	 * @returns {Coverage}
	 */
	function withDomainType(cov, domainType) {
	  checkCoverage(cov);

	  var domainWrapper = function domainWrapper(domain) {
	    var newdomain = {
	      type: DOMAIN,
	      domainType: domainType,
	      axes: domain.axes,
	      referencing: domain.referencing
	    };
	    return newdomain;
	  };

	  var newcov = {
	    type: COVERAGE,
	    domainType: domainType,
	    parameters: cov.parameters,
	    loadDomain: function loadDomain() {
	      return cov.loadDomain().then(domainWrapper);
	    },
	    loadRange: function loadRange(key) {
	      return cov.loadRange(key);
	    },
	    loadRanges: function loadRanges(keys) {
	      return cov.loadRanges(keys);
	    },
	    subsetByIndex: function subsetByIndex(constraints) {
	      return cov.subsetByIndex(constraints).then(function (sub) {
	        return withDomainType(sub, domainType);
	      });
	    },
	    subsetByValue: function subsetByValue(constraints) {
	      return cov.subsetByValue(constraints).then(function (sub) {
	        return withDomainType(sub, domainType);
	      });
	    }
	  };
	  return newcov;
	}

	/**
	 * Tries to transform the given Coverage object into a new one that
	 * conforms to one of the CovJSON domain types.
	 * If multiple domain types match, then the "smaller" one is preferred,
	 * for example, Point instead of Grid.
	 *
	 * The transformation consists of:
	 * - Setting domainType in coverage and domain object
	 * - Renaming domain axes
	 *
	 * @see https://github.com/Reading-eScience-Centre/coveragejson/blob/master/domain-types.md
	 *
	 * @param {Coverage} cov The Coverage object.
	 * @returns {Promise<Coverage>}
	 *   A Promise succeeding with the transformed coverage,
	 *   or failing if no CovJSON domain type matched the input coverage.
	 */
	function asCovJSONDomainType(cov) {
	  return cov.loadDomain().then(function (domain) {

	    // TODO implement me

	  });
	}

	/**
	 * @example
	 * var cov = ...
	 * var mapping = new Map()
	 * mapping.set('lat', 'y').set('lon', 'x')
	 * var newcov = CovUtils.renameAxes(cov, mapping)
	 *
	 * @param {Coverage} cov The coverage.
	 * @param {Map<String,String>} mapping
	 * @returns {Coverage}
	 */
	function renameAxes(cov, mapping) {
	  checkCoverage(cov);
	  mapping = new Map(mapping);
	  var _iteratorNormalCompletion3 = true;
	  var _didIteratorError3 = false;
	  var _iteratorError3 = undefined;

	  try {
	    for (var _iterator3 = cov.axes.keys()[Symbol.iterator](), _step3; !(_iteratorNormalCompletion3 = (_step3 = _iterator3.next()).done); _iteratorNormalCompletion3 = true) {
	      var axisName = _step3.value;

	      if (!mapping.has(axisName)) {
	        mapping.set(axisName, axisName);
	      }
	    }
	  } catch (err) {
	    _didIteratorError3 = true;
	    _iteratorError3 = err;
	  } finally {
	    try {
	      if (!_iteratorNormalCompletion3 && _iterator3.return) {
	        _iterator3.return();
	      }
	    } finally {
	      if (_didIteratorError3) {
	        throw _iteratorError3;
	      }
	    }
	  }

	  var domainWrapper = function domainWrapper(domain) {
	    var newaxes = new Map();
	    var _iteratorNormalCompletion4 = true;
	    var _didIteratorError4 = false;
	    var _iteratorError4 = undefined;

	    try {
	      for (var _iterator4 = mapping[Symbol.iterator](), _step4; !(_iteratorNormalCompletion4 = (_step4 = _iterator4.next()).done); _iteratorNormalCompletion4 = true) {
	        var _step4$value = slicedToArray(_step4.value, 2);

	        var from = _step4$value[0];
	        var to = _step4$value[1];

	        var _domain$axes$get = domain.axes.get(from);

	        var dataType = _domain$axes$get.dataType;
	        var coordinates = _domain$axes$get.coordinates;
	        var values = _domain$axes$get.values;
	        var bounds = _domain$axes$get.bounds;

	        var newaxis = {
	          key: to,
	          dataType: dataType,
	          coordinates: coordinates.map(function (c) {
	            return mapping.has(c) ? mapping.get(c) : c;
	          }),
	          values: values,
	          bounds: bounds
	        };
	        newaxes.set(to, newaxis);
	      }
	    } catch (err) {
	      _didIteratorError4 = true;
	      _iteratorError4 = err;
	    } finally {
	      try {
	        if (!_iteratorNormalCompletion4 && _iterator4.return) {
	          _iterator4.return();
	        }
	      } finally {
	        if (_didIteratorError4) {
	          throw _iteratorError4;
	        }
	      }
	    }

	    var newreferencing = domain.referencing.map(function (_ref) {
	      var coordinates = _ref.coordinates;
	      var system = _ref.system;
	      return {
	        coordinates: coordinates.map(function (c) {
	          return mapping.has(c) ? mapping.get(c) : c;
	        }),
	        system: system
	      };
	    });

	    var newdomain = {
	      type: DOMAIN,
	      domainType: domain.domainType,
	      axes: newaxes,
	      referencing: newreferencing
	    };
	    return newdomain;
	  };

	  // pre-compile for efficiency
	  // get({['lat']: obj['y'], ['lon']: obj['x']})
	  var getObjStr = [].concat(toConsumableArray(mapping)).map(function (_ref2) {
	    var _ref3 = slicedToArray(_ref2, 2);

	    var from = _ref3[0];
	    var to = _ref3[1];
	    return '[\'' + from + '\']:obj[\'' + to + '\']';
	  }).join(',');

	  var rangeWrapper = function rangeWrapper(range) {
	    var get = new Function('range', 'return function get (obj){return range.get({' + getObjStr + '})}')(range); // eslint-disable-line
	    var newrange = {
	      shape: new Map([].concat(toConsumableArray(range.shape)).map(function (_ref4) {
	        var _ref5 = slicedToArray(_ref4, 2);

	        var name = _ref5[0];
	        var len = _ref5[1];
	        return [mapping.get(name), len];
	      })),
	      dataType: range.dataType,
	      get: get
	    };
	    return newrange;
	  };

	  var loadRange = function loadRange(paramKey) {
	    return cov.loadRange(paramKey).then(rangeWrapper);
	  };

	  var loadRanges = function loadRanges(paramKeys) {
	    return cov.loadRanges(paramKeys).then(function (ranges) {
	      return new Map([].concat(toConsumableArray(ranges)).map(function (_ref6) {
	        var _ref7 = slicedToArray(_ref6, 2);

	        var paramKey = _ref7[0];
	        var range = _ref7[1];
	        return [paramKey, rangeWrapper(range)];
	      }));
	    });
	  };

	  var newcov = {
	    type: COVERAGE,
	    domainType: cov.domainType,
	    parameters: cov.parameters,
	    loadDomain: function loadDomain() {
	      return cov.loadDomain().then(domainWrapper);
	    },
	    loadRange: loadRange,
	    loadRanges: loadRanges,
	    subsetByIndex: function subsetByIndex(constraints) {
	      return cov.subsetByIndex(constraints).then(function (sub) {
	        return renameAxes(sub, mapping);
	      });
	    },
	    subsetByValue: function subsetByValue(constraints) {
	      return cov.subsetByValue(constraints).then(function (sub) {
	        return renameAxes(sub, mapping);
	      });
	    }
	  };

	  return newcov;
	}

	/**
	 * @param {Coverage} cov The coverage.
	 * @param {String} key The key of the parameter for which the mapping should be applied.
	 * @param {Function} fn A function getting called as fn(obj, range) where obj is the axis indices object
	 *   and range is the original range object.
	 * @param {String} [dataType] The new data type to use for the range. If omitted, the original type is used.
	 * @returns {Coverage}
	 */
	function mapRange(cov, key, fn, dataType) {
	  checkCoverage(cov);

	  var rangeWrapper = function rangeWrapper(range) {
	    var newrange = {
	      shape: range.shape,
	      dataType: dataType || range.dataType,
	      get: function get(obj) {
	        return fn(obj, range);
	      }
	    };
	    return newrange;
	  };

	  var loadRange = function loadRange(paramKey) {
	    return key === paramKey ? cov.loadRange(paramKey).then(rangeWrapper) : cov.loadRange(paramKey);
	  };

	  var loadRanges = function loadRanges(paramKeys) {
	    return cov.loadRanges(paramKeys).then(function (ranges) {
	      return new Map([].concat(toConsumableArray(ranges)).map(function (_ref8) {
	        var _ref9 = slicedToArray(_ref8, 2);

	        var paramKey = _ref9[0];
	        var range = _ref9[1];
	        return [paramKey, key === paramKey ? rangeWrapper(range) : range];
	      }));
	    });
	  };

	  var newcov = {
	    type: COVERAGE,
	    domainType: cov.domainType,
	    parameters: cov.parameters,
	    loadDomain: function loadDomain() {
	      return cov.loadDomain();
	    },
	    loadRange: loadRange,
	    loadRanges: loadRanges,
	    subsetByIndex: function subsetByIndex(constraints) {
	      return cov.subsetByIndex(constraints).then(function (sub) {
	        return mapRange(sub, key, fn, dataType);
	      });
	    },
	    subsetByValue: function subsetByValue(constraints) {
	      return cov.subsetByValue(constraints).then(function (sub) {
	        return mapRange(sub, key, fn, dataType);
	      });
	    }
	  };

	  return newcov;
	}

	/**
	 *
	 * @example
	 * var cov = ... // has parameters 'NIR', 'red', 'green', 'blue'
	 * var newcov = CovUtils.withDerivedParameter(cov, {
	 *   parameter: {
	 *     key: 'NDVI',
	 *     observedProperty: {
	 *       label: { en: 'Normalized Differenced Vegetation Index' }
	 *     }
	 *   },
	 *   inputParameters: ['NIR','red'],
	 *   dataType: 'float',
	 *   fn: function (obj, nirRange, redRange) {
	 *     var nir = nirRange.get(obj)
	 *     var red = redRange.get(obj)
	 *     if (nir === null || red === null) return null
	 *     return (nir - red) / (nir + red)
	 *   }
	 * })
	 */
	function withDerivedParameter(cov, options) {
	  checkCoverage(cov);
	  var parameter = options.parameter;
	  var inputParameters = options.inputParameters;
	  var _options$dataType = options.dataType;
	  var dataType = _options$dataType === undefined ? 'float' : _options$dataType;
	  var fn = options.fn;


	  var parameters = new Map(cov.parameters);
	  parameters.set(parameter.key, parameter);

	  var loadDerivedRange = function loadDerivedRange() {
	    return cov.loadRanges(inputParameters).then(function (inputRanges) {
	      var inputRangesArr = inputParameters.map(function (key) {
	        return inputRanges.get(key);
	      });
	      var shape = inputRangesArr[0].shape;
	      var range = {
	        shape: shape,
	        dataType: dataType,
	        get: function get(obj) {
	          return fn.apply(undefined, [obj].concat(toConsumableArray(inputRangesArr)));
	        }
	      };
	      return range;
	    });
	  };

	  var loadRange = function loadRange(paramKey) {
	    return parameter.key === paramKey ? loadDerivedRange() : cov.loadRange(paramKey);
	  };

	  var newcov = {
	    type: COVERAGE,
	    domainType: cov.domainType,
	    parameters: parameters,
	    loadDomain: function loadDomain() {
	      return cov.loadDomain();
	    },
	    loadRange: loadRange,
	    subsetByIndex: function subsetByIndex(constraints) {
	      return cov.subsetByIndex(constraints).then(function (sub) {
	        return withDerivedParameter(sub, options);
	      });
	    },
	    subsetByValue: function subsetByValue(constraints) {
	      return cov.subsetByValue(constraints).then(function (sub) {
	        return withDerivedParameter(sub, options);
	      });
	    }
	  };
	  addLoadRangesFunction(newcov);

	  return newcov;
	}

	/**
	 *
	 * @example
	 * var cov = ... // has parameters 'NIR', 'red', 'green', 'blue'
	 * var newcov = CovUtils.withSimpleDerivedParameter(cov, {
	 *   parameter: {
	 *     key: 'NDVI',
	 *     observedProperty: {
	 *       label: { en: 'Normalized Differenced Vegetation Index' }
	 *     }
	 *   },
	 *   inputParameters: ['NIR','red'],
	 *   dataType: 'float',
	 *   fn: function (nir, red) {
	 *     return (nir - red) / (nir + red)
	 *   }
	 * })
	 */
	function withSimpleDerivedParameter(cov, options) {
	  var parameter = options.parameter;
	  var inputParameters = options.inputParameters;
	  var dataType = options.dataType;
	  var _fn = options.fn;

	  var options_ = {
	    parameter: parameter,
	    inputParameters: inputParameters,
	    dataType: dataType,
	    // TODO pre-compile if too slow
	    fn: function fn(obj) {
	      for (var _len = arguments.length, ranges = Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
	        ranges[_key - 1] = arguments[_key];
	      }

	      var vals = inputParameters.map(function (_, i) {
	        return ranges[i].get(obj);
	      });
	      if (vals.some(function (val) {
	        return val === null;
	      })) {
	        return null;
	      }
	      return _fn.apply(undefined, toConsumableArray(vals));
	    }
	  };
	  return withDerivedParameter(cov, options_);
	}

	/**
	 * Adds a basic query() function to the coverage collection object.
	 * Note that this does not support paging.
	 */
	function addCollectionQueryFunction(collection) {
	  if (collection.paging) {
	    throw new Error('Paged collections not supported');
	  }
	  collection.query = function () {
	    return new CollectionQuery(collection);
	  };
	}

	var CollectionQuery = function () {
	  /**
	   * @param {CoverageCollection} collection
	   */

	  function CollectionQuery(collection) {
	    classCallCheck(this, CollectionQuery);

	    this._collection = collection;
	    this._filter = {};
	    this._subset = {};
	  }

	  /**
	   * Matching mode: intersect
	   *
	   * Supports ISO8601 date string axes.
	   * All other string-type axes are compared alphabetically.
	   *
	   * @example
	   * collection.query().filter({
	   *   't': {start: '2015-01-01T01:00:00', stop: '2015-01-01T02:00:00'}
	   * }).execute().then(filteredCollection => {
	   *   console.log(filteredCollection.coverages.length)
	   * })
	   * @param {Object} spec
	   * @return {CollectionQuery}
	   */


	  createClass(CollectionQuery, [{
	    key: 'filter',
	    value: function filter(spec) {
	      mergeInto(spec, this._filter);
	      return this;
	    }

	    /**
	     * Subset coverages by domain values.
	     *
	     * Equivalent to calling {@link Coverage.subsetByValue}(spec) on each
	     * coverage in the collection.
	     *
	     * @param {Object} spec
	     * @return {CollectionQuery}
	     */

	  }, {
	    key: 'subset',
	    value: function subset(spec) {
	      mergeInto(spec, this._subset);
	      return this;
	    }

	    /**
	     * Applies the query operators and returns
	     * a Promise that succeeds with a new CoverageCollection.
	     *
	     * @return {Promise<CoverageCollection>}
	     */

	  }, {
	    key: 'execute',
	    value: function execute() {
	      var _this = this;

	      var coll = this._collection;
	      var newcoll = {
	        type: COVERAGECOLLECTION,
	        coverages: [],
	        parameters: coll.parameters,
	        domainType: coll.domainType
	      };

	      var promises = [];
	      var _iteratorNormalCompletion = true;
	      var _didIteratorError = false;
	      var _iteratorError = undefined;

	      try {
	        var _loop = function _loop() {
	          var cov = _step.value;

	          promises.push(cov.loadDomain().then(function (domain) {
	            if (!matchesFilter(domain, _this._filter)) {
	              return;
	            }

	            if (Object.keys(_this._subset).length === 0) {
	              newcoll.coverages.push(cov);
	            } else {
	              return cov.subsetByValue(_this._subset).then(function (subsetted) {
	                newcoll.coverages.push(subsetted);
	              });
	            }
	          }));
	        };

	        for (var _iterator = coll.coverages[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
	          _loop();
	        }
	      } catch (err) {
	        _didIteratorError = true;
	        _iteratorError = err;
	      } finally {
	        try {
	          if (!_iteratorNormalCompletion && _iterator.return) {
	            _iterator.return();
	          }
	        } finally {
	          if (_didIteratorError) {
	            throw _iteratorError;
	          }
	        }
	      }

	      return Promise.all(promises).then(function () {
	        newcoll.query = function () {
	          return new CollectionQuery(newcoll);
	        };
	        return newcoll;
	      });
	    }
	  }]);
	  return CollectionQuery;
	}();

	function matchesFilter(domain, filter) {
	  var _iteratorNormalCompletion2 = true;
	  var _didIteratorError2 = false;
	  var _iteratorError2 = undefined;

	  try {
	    for (var _iterator2 = Object.keys(filter)[Symbol.iterator](), _step2; !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
	      var axisName = _step2.value;

	      var condition = filter[axisName];
	      if (!domain.axes.has(axisName)) {
	        throw new Error('Axis "' + axisName + '" does not exist');
	      }
	      var axis = domain.axes.get(axisName);
	      var vals = axis.values;

	      var min = vals[0];
	      var max = vals[vals.length - 1];

	      if (typeof min !== 'number' && typeof min !== 'string') {
	        throw new Error('Can only filter primitive axis values');
	      }
	      var start = condition.start;
	      var stop = condition.stop;

	      // special handling

	      if (isISODateAxis(domain, axisName)) {
	        var _ref = [asTime(min), asTime(max)];
	        min = _ref[0];
	        max = _ref[1];
	        var _ref2 = [asTime(start), asTime(stop)];
	        start = _ref2[0];
	        stop = _ref2[1];
	      } else if (isLongitudeAxis(domain, axisName)) {
	        var lonWrapper = getLongitudeWrapper(domain, axisName);var _ref3 = [lonWrapper(start), lonWrapper(stop)];
	        start = _ref3[0];
	        stop = _ref3[1];
	      }

	      if (min > max) {
	        var _ref4 = [max, min];
	        min = _ref4[0];
	        max = _ref4[1];
	      }
	      if (max < start || stop < min) {
	        return false;
	      }
	    }
	  } catch (err) {
	    _didIteratorError2 = true;
	    _iteratorError2 = err;
	  } finally {
	    try {
	      if (!_iteratorNormalCompletion2 && _iterator2.return) {
	        _iterator2.return();
	      }
	    } finally {
	      if (_didIteratorError2) {
	        throw _iteratorError2;
	      }
	    }
	  }

	  return true;
	}

	function mergeInto(inputObj, targetObj) {
	  var _iteratorNormalCompletion3 = true;
	  var _didIteratorError3 = false;
	  var _iteratorError3 = undefined;

	  try {
	    for (var _iterator3 = Object.keys(inputObj)[Symbol.iterator](), _step3; !(_iteratorNormalCompletion3 = (_step3 = _iterator3.next()).done); _iteratorNormalCompletion3 = true) {
	      var k = _step3.value;

	      targetObj[k] = inputObj[k];
	    }
	  } catch (err) {
	    _didIteratorError3 = true;
	    _iteratorError3 = err;
	  } finally {
	    try {
	      if (!_iteratorNormalCompletion3 && _iterator3.return) {
	        _iterator3.return();
	      }
	    } finally {
	      if (_didIteratorError3) {
	        throw _iteratorError3;
	      }
	    }
	  }
	}

	var searchBounds = createCommonjsModule(function (module) {
	"use strict"

	function compileSearch(funcName, predicate, reversed, extraArgs, useNdarray, earlyOut) {
	  var code = [
	    "function ", funcName, "(a,l,h,", extraArgs.join(","),  "){",
	earlyOut ? "" : "var i=", (reversed ? "l-1" : "h+1"),
	";while(l<=h){\
var m=(l+h)>>>1,x=a", useNdarray ? ".get(m)" : "[m]"]
	  if(earlyOut) {
	    if(predicate.indexOf("c") < 0) {
	      code.push(";if(x===y){return m}else if(x<=y){")
	    } else {
	      code.push(";var p=c(x,y);if(p===0){return m}else if(p<=0){")
	    }
	  } else {
	    code.push(";if(", predicate, "){i=m;")
	  }
	  if(reversed) {
	    code.push("l=m+1}else{h=m-1}")
	  } else {
	    code.push("h=m-1}else{l=m+1}")
	  }
	  code.push("}")
	  if(earlyOut) {
	    code.push("return -1};")
	  } else {
	    code.push("return i};")
	  }
	  return code.join("")
	}

	function compileBoundsSearch(predicate, reversed, suffix, earlyOut) {
	  var result = new Function([
	  compileSearch("A", "x" + predicate + "y", reversed, ["y"], false, earlyOut),
	  compileSearch("B", "x" + predicate + "y", reversed, ["y"], true, earlyOut),
	  compileSearch("P", "c(x,y)" + predicate + "0", reversed, ["y", "c"], false, earlyOut),
	  compileSearch("Q", "c(x,y)" + predicate + "0", reversed, ["y", "c"], true, earlyOut),
	"function dispatchBsearch", suffix, "(a,y,c,l,h){\
if(a.shape){\
if(typeof(c)==='function'){\
return Q(a,(l===undefined)?0:l|0,(h===undefined)?a.shape[0]-1:h|0,y,c)\
}else{\
return B(a,(c===undefined)?0:c|0,(l===undefined)?a.shape[0]-1:l|0,y)\
}}else{\
if(typeof(c)==='function'){\
return P(a,(l===undefined)?0:l|0,(h===undefined)?a.length-1:h|0,y,c)\
}else{\
return A(a,(c===undefined)?0:c|0,(l===undefined)?a.length-1:l|0,y)\
}}}\
return dispatchBsearch", suffix].join(""))
	  return result()
	}

	module.exports = {
	  ge: compileBoundsSearch(">=", false, "GE"),
	  gt: compileBoundsSearch(">", false, "GT"),
	  lt: compileBoundsSearch("<", true, "LT"),
	  le: compileBoundsSearch("<=", true, "LE"),
	  eq: compileBoundsSearch("-", true, "EQ", true)
	}
	});

	var require$$3$8 = (searchBounds && typeof searchBounds === 'object' && 'default' in searchBounds ? searchBounds['default'] : searchBounds);

	var intervalTree = createCommonjsModule(function (module) {
	"use strict"

	var bounds = require$$3$8

	var NOT_FOUND = 0
	var SUCCESS = 1
	var EMPTY = 2

	module.exports = createWrapper

	function IntervalTreeNode(mid, left, right, leftPoints, rightPoints) {
	  this.mid = mid
	  this.left = left
	  this.right = right
	  this.leftPoints = leftPoints
	  this.rightPoints = rightPoints
	  this.count = (left ? left.count : 0) + (right ? right.count : 0) + leftPoints.length
	}

	var proto = IntervalTreeNode.prototype

	function copy(a, b) {
	  a.mid = b.mid
	  a.left = b.left
	  a.right = b.right
	  a.leftPoints = b.leftPoints
	  a.rightPoints = b.rightPoints
	  a.count = b.count
	}

	function rebuild(node, intervals) {
	  var ntree = createIntervalTree(intervals)
	  node.mid = ntree.mid
	  node.left = ntree.left
	  node.right = ntree.right
	  node.leftPoints = ntree.leftPoints
	  node.rightPoints = ntree.rightPoints
	  node.count = ntree.count
	}

	function rebuildWithInterval(node, interval) {
	  var intervals = node.intervals([])
	  intervals.push(interval)
	  rebuild(node, intervals)    
	}

	function rebuildWithoutInterval(node, interval) {
	  var intervals = node.intervals([])
	  var idx = intervals.indexOf(interval)
	  if(idx < 0) {
	    return NOT_FOUND
	  }
	  intervals.splice(idx, 1)
	  rebuild(node, intervals)
	  return SUCCESS
	}

	proto.intervals = function(result) {
	  result.push.apply(result, this.leftPoints)
	  if(this.left) {
	    this.left.intervals(result)
	  }
	  if(this.right) {
	    this.right.intervals(result)
	  }
	  return result
	}

	proto.insert = function(interval) {
	  var weight = this.count - this.leftPoints.length
	  this.count += 1
	  if(interval[1] < this.mid) {
	    if(this.left) {
	      if(4*(this.left.count+1) > 3*(weight+1)) {
	        rebuildWithInterval(this, interval)
	      } else {
	        this.left.insert(interval)
	      }
	    } else {
	      this.left = createIntervalTree([interval])
	    }
	  } else if(interval[0] > this.mid) {
	    if(this.right) {
	      if(4*(this.right.count+1) > 3*(weight+1)) {
	        rebuildWithInterval(this, interval)
	      } else {
	        this.right.insert(interval)
	      }
	    } else {
	      this.right = createIntervalTree([interval])
	    }
	  } else {
	    var l = bounds.ge(this.leftPoints, interval, compareBegin)
	    var r = bounds.ge(this.rightPoints, interval, compareEnd)
	    this.leftPoints.splice(l, 0, interval)
	    this.rightPoints.splice(r, 0, interval)
	  }
	}

	proto.remove = function(interval) {
	  var weight = this.count - this.leftPoints
	  if(interval[1] < this.mid) {
	    if(!this.left) {
	      return NOT_FOUND
	    }
	    var rw = this.right ? this.right.count : 0
	    if(4 * rw > 3 * (weight-1)) {
	      return rebuildWithoutInterval(this, interval)
	    }
	    var r = this.left.remove(interval)
	    if(r === EMPTY) {
	      this.left = null
	      this.count -= 1
	      return SUCCESS
	    } else if(r === SUCCESS) {
	      this.count -= 1
	    }
	    return r
	  } else if(interval[0] > this.mid) {
	    if(!this.right) {
	      return NOT_FOUND
	    }
	    var lw = this.left ? this.left.count : 0
	    if(4 * lw > 3 * (weight-1)) {
	      return rebuildWithoutInterval(this, interval)
	    }
	    var r = this.right.remove(interval)
	    if(r === EMPTY) {
	      this.right = null
	      this.count -= 1
	      return SUCCESS
	    } else if(r === SUCCESS) {
	      this.count -= 1
	    }
	    return r
	  } else {
	    if(this.count === 1) {
	      if(this.leftPoints[0] === interval) {
	        return EMPTY
	      } else {
	        return NOT_FOUND
	      }
	    }
	    if(this.leftPoints.length === 1 && this.leftPoints[0] === interval) {
	      if(this.left && this.right) {
	        var p = this
	        var n = this.left
	        while(n.right) {
	          p = n
	          n = n.right
	        }
	        if(p === this) {
	          n.right = this.right
	        } else {
	          var l = this.left
	          var r = this.right
	          p.count -= n.count
	          p.right = n.left
	          n.left = l
	          n.right = r
	        }
	        copy(this, n)
	        this.count = (this.left?this.left.count:0) + (this.right?this.right.count:0) + this.leftPoints.length
	      } else if(this.left) {
	        copy(this, this.left)
	      } else {
	        copy(this, this.right)
	      }
	      return SUCCESS
	    }
	    for(var l = bounds.ge(this.leftPoints, interval, compareBegin); l<this.leftPoints.length; ++l) {
	      if(this.leftPoints[l][0] !== interval[0]) {
	        break
	      }
	      if(this.leftPoints[l] === interval) {
	        this.count -= 1
	        this.leftPoints.splice(l, 1)
	        for(var r = bounds.ge(this.rightPoints, interval, compareEnd); r<this.rightPoints.length; ++r) {
	          if(this.rightPoints[r][1] !== interval[1]) {
	            break
	          } else if(this.rightPoints[r] === interval) {
	            this.rightPoints.splice(r, 1)
	            return SUCCESS
	          }
	        }
	      }
	    }
	    return NOT_FOUND
	  }
	}

	function reportLeftRange(arr, hi, cb) {
	  for(var i=0; i<arr.length && arr[i][0] <= hi; ++i) {
	    var r = cb(arr[i])
	    if(r) { return r }
	  }
	}

	function reportRightRange(arr, lo, cb) {
	  for(var i=arr.length-1; i>=0 && arr[i][1] >= lo; --i) {
	    var r = cb(arr[i])
	    if(r) { return r }
	  }
	}

	function reportRange(arr, cb) {
	  for(var i=0; i<arr.length; ++i) {
	    var r = cb(arr[i])
	    if(r) { return r }
	  }
	}

	proto.queryPoint = function(x, cb) {
	  if(x < this.mid) {
	    if(this.left) {
	      var r = this.left.queryPoint(x, cb)
	      if(r) { return r }
	    }
	    return reportLeftRange(this.leftPoints, x, cb)
	  } else if(x > this.mid) {
	    if(this.right) {
	      var r = this.right.queryPoint(x, cb)
	      if(r) { return r }
	    }
	    return reportRightRange(this.rightPoints, x, cb)
	  } else {
	    return reportRange(this.leftPoints, cb)
	  }
	}

	proto.queryInterval = function(lo, hi, cb) {
	  if(lo < this.mid && this.left) {
	    var r = this.left.queryInterval(lo, hi, cb)
	    if(r) { return r }
	  }
	  if(hi > this.mid && this.right) {
	    var r = this.right.queryInterval(lo, hi, cb)
	    if(r) { return r }
	  }
	  if(hi < this.mid) {
	    return reportLeftRange(this.leftPoints, hi, cb)
	  } else if(lo > this.mid) {
	    return reportRightRange(this.rightPoints, lo, cb)
	  } else {
	    return reportRange(this.leftPoints, cb)
	  }
	}

	function compareNumbers(a, b) {
	  return a - b
	}

	function compareBegin(a, b) {
	  var d = a[0] - b[0]
	  if(d) { return d }
	  return a[1] - b[1]
	}

	function compareEnd(a, b) {
	  var d = a[1] - b[1]
	  if(d) { return d }
	  return a[0] - b[0]
	}

	function createIntervalTree(intervals) {
	  if(intervals.length === 0) {
	    return null
	  }
	  var pts = []
	  for(var i=0; i<intervals.length; ++i) {
	    pts.push(intervals[i][0], intervals[i][1])
	  }
	  pts.sort(compareNumbers)

	  var mid = pts[pts.length>>1]

	  var leftIntervals = []
	  var rightIntervals = []
	  var centerIntervals = []
	  for(var i=0; i<intervals.length; ++i) {
	    var s = intervals[i]
	    if(s[1] < mid) {
	      leftIntervals.push(s)
	    } else if(mid < s[0]) {
	      rightIntervals.push(s)
	    } else {
	      centerIntervals.push(s)
	    }
	  }

	  //Split center intervals
	  var leftPoints = centerIntervals
	  var rightPoints = centerIntervals.slice()
	  leftPoints.sort(compareBegin)
	  rightPoints.sort(compareEnd)

	  return new IntervalTreeNode(mid, 
	    createIntervalTree(leftIntervals),
	    createIntervalTree(rightIntervals),
	    leftPoints,
	    rightPoints)
	}

	//User friendly wrapper that makes it possible to support empty trees
	function IntervalTree(root) {
	  this.root = root
	}

	var tproto = IntervalTree.prototype

	tproto.insert = function(interval) {
	  if(this.root) {
	    this.root.insert(interval)
	  } else {
	    this.root = new IntervalTreeNode(interval[0], null, null, [interval], [interval])
	  }
	}

	tproto.remove = function(interval) {
	  if(this.root) {
	    var r = this.root.remove(interval)
	    if(r === EMPTY) {
	      this.root = null
	    }
	    return r !== NOT_FOUND
	  }
	  return false
	}

	tproto.queryPoint = function(p, cb) {
	  if(this.root) {
	    return this.root.queryPoint(p, cb)
	  }
	}

	tproto.queryInterval = function(lo, hi, cb) {
	  if(lo <= hi && this.root) {
	    return this.root.queryInterval(lo, hi, cb)
	  }
	}

	Object.defineProperty(tproto, "count", {
	  get: function() {
	    if(this.root) {
	      return this.root.count
	    }
	    return 0
	  }
	})

	Object.defineProperty(tproto, "intervals", {
	  get: function() {
	    if(this.root) {
	      return this.root.intervals([])
	    }
	    return []
	  }
	})

	function createWrapper(intervals) {
	  if(!intervals || intervals.length === 0) {
	    return new IntervalTree(null)
	  }
	  return new IntervalTree(createIntervalTree(intervals))
	}
	});

	var require$$1$13 = (intervalTree && typeof intervalTree === 'object' && 'default' in intervalTree ? intervalTree['default'] : intervalTree);

	var robustDiff = createCommonjsModule(function (module) {
	"use strict"

	module.exports = robustSubtract

	//Easy case: Add two scalars
	function scalarScalar(a, b) {
	  var x = a + b
	  var bv = x - a
	  var av = x - bv
	  var br = b - bv
	  var ar = a - av
	  var y = ar + br
	  if(y) {
	    return [y, x]
	  }
	  return [x]
	}

	function robustSubtract(e, f) {
	  var ne = e.length|0
	  var nf = f.length|0
	  if(ne === 1 && nf === 1) {
	    return scalarScalar(e[0], -f[0])
	  }
	  var n = ne + nf
	  var g = new Array(n)
	  var count = 0
	  var eptr = 0
	  var fptr = 0
	  var abs = Math.abs
	  var ei = e[eptr]
	  var ea = abs(ei)
	  var fi = -f[fptr]
	  var fa = abs(fi)
	  var a, b
	  if(ea < fa) {
	    b = ei
	    eptr += 1
	    if(eptr < ne) {
	      ei = e[eptr]
	      ea = abs(ei)
	    }
	  } else {
	    b = fi
	    fptr += 1
	    if(fptr < nf) {
	      fi = -f[fptr]
	      fa = abs(fi)
	    }
	  }
	  if((eptr < ne && ea < fa) || (fptr >= nf)) {
	    a = ei
	    eptr += 1
	    if(eptr < ne) {
	      ei = e[eptr]
	      ea = abs(ei)
	    }
	  } else {
	    a = fi
	    fptr += 1
	    if(fptr < nf) {
	      fi = -f[fptr]
	      fa = abs(fi)
	    }
	  }
	  var x = a + b
	  var bv = x - a
	  var y = b - bv
	  var q0 = y
	  var q1 = x
	  var _x, _bv, _av, _br, _ar
	  while(eptr < ne && fptr < nf) {
	    if(ea < fa) {
	      a = ei
	      eptr += 1
	      if(eptr < ne) {
	        ei = e[eptr]
	        ea = abs(ei)
	      }
	    } else {
	      a = fi
	      fptr += 1
	      if(fptr < nf) {
	        fi = -f[fptr]
	        fa = abs(fi)
	      }
	    }
	    b = q0
	    x = a + b
	    bv = x - a
	    y = b - bv
	    if(y) {
	      g[count++] = y
	    }
	    _x = q1 + x
	    _bv = _x - q1
	    _av = _x - _bv
	    _br = x - _bv
	    _ar = q1 - _av
	    q0 = _ar + _br
	    q1 = _x
	  }
	  while(eptr < ne) {
	    a = ei
	    b = q0
	    x = a + b
	    bv = x - a
	    y = b - bv
	    if(y) {
	      g[count++] = y
	    }
	    _x = q1 + x
	    _bv = _x - q1
	    _av = _x - _bv
	    _br = x - _bv
	    _ar = q1 - _av
	    q0 = _ar + _br
	    q1 = _x
	    eptr += 1
	    if(eptr < ne) {
	      ei = e[eptr]
	    }
	  }
	  while(fptr < nf) {
	    a = fi
	    b = q0
	    x = a + b
	    bv = x - a
	    y = b - bv
	    if(y) {
	      g[count++] = y
	    } 
	    _x = q1 + x
	    _bv = _x - q1
	    _av = _x - _bv
	    _br = x - _bv
	    _ar = q1 - _av
	    q0 = _ar + _br
	    q1 = _x
	    fptr += 1
	    if(fptr < nf) {
	      fi = -f[fptr]
	    }
	  }
	  if(q0) {
	    g[count++] = q0
	  }
	  if(q1) {
	    g[count++] = q1
	  }
	  if(!count) {
	    g[count++] = 0.0  
	  }
	  g.length = count
	  return g
	}
	});

	var require$$0$21 = (robustDiff && typeof robustDiff === 'object' && 'default' in robustDiff ? robustDiff['default'] : robustDiff);

	var twoSum = createCommonjsModule(function (module) {
	"use strict"

	module.exports = fastTwoSum

	function fastTwoSum(a, b, result) {
		var x = a + b
		var bv = x - a
		var av = x - bv
		var br = b - bv
		var ar = a - av
		if(result) {
			result[0] = ar + br
			result[1] = x
			return result
		}
		return [ar+br, x]
	}
	});

	var require$$0$22 = (twoSum && typeof twoSum === 'object' && 'default' in twoSum ? twoSum['default'] : twoSum);

	var twoProduct = createCommonjsModule(function (module) {
	"use strict"

	module.exports = twoProduct

	var SPLITTER = +(Math.pow(2, 27) + 1.0)

	function twoProduct(a, b, result) {
	  var x = a * b

	  var c = SPLITTER * a
	  var abig = c - a
	  var ahi = c - abig
	  var alo = a - ahi

	  var d = SPLITTER * b
	  var bbig = d - b
	  var bhi = d - bbig
	  var blo = b - bhi

	  var err1 = x - (ahi * bhi)
	  var err2 = err1 - (alo * bhi)
	  var err3 = err2 - (ahi * blo)

	  var y = alo * blo - err3

	  if(result) {
	    result[0] = y
	    result[1] = x
	    return result
	  }

	  return [ y, x ]
	}
	});

	var require$$1$15 = (twoProduct && typeof twoProduct === 'object' && 'default' in twoProduct ? twoProduct['default'] : twoProduct);

	var robustScale = createCommonjsModule(function (module) {
	"use strict"

	var twoProduct = require$$1$15
	var twoSum = require$$0$22

	module.exports = scaleLinearExpansion

	function scaleLinearExpansion(e, scale) {
	  var n = e.length
	  if(n === 1) {
	    var ts = twoProduct(e[0], scale)
	    if(ts[0]) {
	      return ts
	    }
	    return [ ts[1] ]
	  }
	  var g = new Array(2 * n)
	  var q = [0.1, 0.1]
	  var t = [0.1, 0.1]
	  var count = 0
	  twoProduct(e[0], scale, q)
	  if(q[0]) {
	    g[count++] = q[0]
	  }
	  for(var i=1; i<n; ++i) {
	    twoProduct(e[i], scale, t)
	    var pq = q[1]
	    twoSum(pq, t[0], q)
	    if(q[0]) {
	      g[count++] = q[0]
	    }
	    var a = t[1]
	    var b = q[1]
	    var x = a + b
	    var bv = x - a
	    var y = b - bv
	    q[1] = x
	    if(y) {
	      g[count++] = y
	    }
	  }
	  if(q[1]) {
	    g[count++] = q[1]
	  }
	  if(count === 0) {
	    g[count++] = 0.0
	  }
	  g.length = count
	  return g
	}
	});

	var require$$1$14 = (robustScale && typeof robustScale === 'object' && 'default' in robustScale ? robustScale['default'] : robustScale);

	var robustSum = createCommonjsModule(function (module) {
	"use strict"

	module.exports = linearExpansionSum

	//Easy case: Add two scalars
	function scalarScalar(a, b) {
	  var x = a + b
	  var bv = x - a
	  var av = x - bv
	  var br = b - bv
	  var ar = a - av
	  var y = ar + br
	  if(y) {
	    return [y, x]
	  }
	  return [x]
	}

	function linearExpansionSum(e, f) {
	  var ne = e.length|0
	  var nf = f.length|0
	  if(ne === 1 && nf === 1) {
	    return scalarScalar(e[0], f[0])
	  }
	  var n = ne + nf
	  var g = new Array(n)
	  var count = 0
	  var eptr = 0
	  var fptr = 0
	  var abs = Math.abs
	  var ei = e[eptr]
	  var ea = abs(ei)
	  var fi = f[fptr]
	  var fa = abs(fi)
	  var a, b
	  if(ea < fa) {
	    b = ei
	    eptr += 1
	    if(eptr < ne) {
	      ei = e[eptr]
	      ea = abs(ei)
	    }
	  } else {
	    b = fi
	    fptr += 1
	    if(fptr < nf) {
	      fi = f[fptr]
	      fa = abs(fi)
	    }
	  }
	  if((eptr < ne && ea < fa) || (fptr >= nf)) {
	    a = ei
	    eptr += 1
	    if(eptr < ne) {
	      ei = e[eptr]
	      ea = abs(ei)
	    }
	  } else {
	    a = fi
	    fptr += 1
	    if(fptr < nf) {
	      fi = f[fptr]
	      fa = abs(fi)
	    }
	  }
	  var x = a + b
	  var bv = x - a
	  var y = b - bv
	  var q0 = y
	  var q1 = x
	  var _x, _bv, _av, _br, _ar
	  while(eptr < ne && fptr < nf) {
	    if(ea < fa) {
	      a = ei
	      eptr += 1
	      if(eptr < ne) {
	        ei = e[eptr]
	        ea = abs(ei)
	      }
	    } else {
	      a = fi
	      fptr += 1
	      if(fptr < nf) {
	        fi = f[fptr]
	        fa = abs(fi)
	      }
	    }
	    b = q0
	    x = a + b
	    bv = x - a
	    y = b - bv
	    if(y) {
	      g[count++] = y
	    }
	    _x = q1 + x
	    _bv = _x - q1
	    _av = _x - _bv
	    _br = x - _bv
	    _ar = q1 - _av
	    q0 = _ar + _br
	    q1 = _x
	  }
	  while(eptr < ne) {
	    a = ei
	    b = q0
	    x = a + b
	    bv = x - a
	    y = b - bv
	    if(y) {
	      g[count++] = y
	    }
	    _x = q1 + x
	    _bv = _x - q1
	    _av = _x - _bv
	    _br = x - _bv
	    _ar = q1 - _av
	    q0 = _ar + _br
	    q1 = _x
	    eptr += 1
	    if(eptr < ne) {
	      ei = e[eptr]
	    }
	  }
	  while(fptr < nf) {
	    a = fi
	    b = q0
	    x = a + b
	    bv = x - a
	    y = b - bv
	    if(y) {
	      g[count++] = y
	    } 
	    _x = q1 + x
	    _bv = _x - q1
	    _av = _x - _bv
	    _br = x - _bv
	    _ar = q1 - _av
	    q0 = _ar + _br
	    q1 = _x
	    fptr += 1
	    if(fptr < nf) {
	      fi = f[fptr]
	    }
	  }
	  if(q0) {
	    g[count++] = q0
	  }
	  if(q1) {
	    g[count++] = q1
	  }
	  if(!count) {
	    g[count++] = 0.0  
	  }
	  g.length = count
	  return g
	}
	});

	var require$$2$7 = (robustSum && typeof robustSum === 'object' && 'default' in robustSum ? robustSum['default'] : robustSum);

	var orientation = createCommonjsModule(function (module) {
	"use strict"

	var twoProduct = require$$1$15
	var robustSum = require$$2$7
	var robustScale = require$$1$14
	var robustSubtract = require$$0$21

	var NUM_EXPAND = 5

	var EPSILON     = 1.1102230246251565e-16
	var ERRBOUND3   = (3.0 + 16.0 * EPSILON) * EPSILON
	var ERRBOUND4   = (7.0 + 56.0 * EPSILON) * EPSILON

	function cofactor(m, c) {
	  var result = new Array(m.length-1)
	  for(var i=1; i<m.length; ++i) {
	    var r = result[i-1] = new Array(m.length-1)
	    for(var j=0,k=0; j<m.length; ++j) {
	      if(j === c) {
	        continue
	      }
	      r[k++] = m[i][j]
	    }
	  }
	  return result
	}

	function matrix(n) {
	  var result = new Array(n)
	  for(var i=0; i<n; ++i) {
	    result[i] = new Array(n)
	    for(var j=0; j<n; ++j) {
	      result[i][j] = ["m", j, "[", (n-i-1), "]"].join("")
	    }
	  }
	  return result
	}

	function sign(n) {
	  if(n & 1) {
	    return "-"
	  }
	  return ""
	}

	function generateSum(expr) {
	  if(expr.length === 1) {
	    return expr[0]
	  } else if(expr.length === 2) {
	    return ["sum(", expr[0], ",", expr[1], ")"].join("")
	  } else {
	    var m = expr.length>>1
	    return ["sum(", generateSum(expr.slice(0, m)), ",", generateSum(expr.slice(m)), ")"].join("")
	  }
	}

	function determinant(m) {
	  if(m.length === 2) {
	    return [["sum(prod(", m[0][0], ",", m[1][1], "),prod(-", m[0][1], ",", m[1][0], "))"].join("")]
	  } else {
	    var expr = []
	    for(var i=0; i<m.length; ++i) {
	      expr.push(["scale(", generateSum(determinant(cofactor(m, i))), ",", sign(i), m[0][i], ")"].join(""))
	    }
	    return expr
	  }
	}

	function orientation(n) {
	  var pos = []
	  var neg = []
	  var m = matrix(n)
	  var args = []
	  for(var i=0; i<n; ++i) {
	    if((i&1)===0) {
	      pos.push.apply(pos, determinant(cofactor(m, i)))
	    } else {
	      neg.push.apply(neg, determinant(cofactor(m, i)))
	    }
	    args.push("m" + i)
	  }
	  var posExpr = generateSum(pos)
	  var negExpr = generateSum(neg)
	  var funcName = "orientation" + n + "Exact"
	  var code = ["function ", funcName, "(", args.join(), "){var p=", posExpr, ",n=", negExpr, ",d=sub(p,n);\
return d[d.length-1];};return ", funcName].join("")
	  var proc = new Function("sum", "prod", "scale", "sub", code)
	  return proc(robustSum, twoProduct, robustScale, robustSubtract)
	}

	var orientation3Exact = orientation(3)
	var orientation4Exact = orientation(4)

	var CACHED = [
	  function orientation0() { return 0 },
	  function orientation1() { return 0 },
	  function orientation2(a, b) { 
	    return b[0] - a[0]
	  },
	  function orientation3(a, b, c) {
	    var l = (a[1] - c[1]) * (b[0] - c[0])
	    var r = (a[0] - c[0]) * (b[1] - c[1])
	    var det = l - r
	    var s
	    if(l > 0) {
	      if(r <= 0) {
	        return det
	      } else {
	        s = l + r
	      }
	    } else if(l < 0) {
	      if(r >= 0) {
	        return det
	      } else {
	        s = -(l + r)
	      }
	    } else {
	      return det
	    }
	    var tol = ERRBOUND3 * s
	    if(det >= tol || det <= -tol) {
	      return det
	    }
	    return orientation3Exact(a, b, c)
	  },
	  function orientation4(a,b,c,d) {
	    var adx = a[0] - d[0]
	    var bdx = b[0] - d[0]
	    var cdx = c[0] - d[0]
	    var ady = a[1] - d[1]
	    var bdy = b[1] - d[1]
	    var cdy = c[1] - d[1]
	    var adz = a[2] - d[2]
	    var bdz = b[2] - d[2]
	    var cdz = c[2] - d[2]
	    var bdxcdy = bdx * cdy
	    var cdxbdy = cdx * bdy
	    var cdxady = cdx * ady
	    var adxcdy = adx * cdy
	    var adxbdy = adx * bdy
	    var bdxady = bdx * ady
	    var det = adz * (bdxcdy - cdxbdy) 
	            + bdz * (cdxady - adxcdy)
	            + cdz * (adxbdy - bdxady)
	    var permanent = (Math.abs(bdxcdy) + Math.abs(cdxbdy)) * Math.abs(adz)
	                  + (Math.abs(cdxady) + Math.abs(adxcdy)) * Math.abs(bdz)
	                  + (Math.abs(adxbdy) + Math.abs(bdxady)) * Math.abs(cdz)
	    var tol = ERRBOUND4 * permanent
	    if ((det > tol) || (-det > tol)) {
	      return det
	    }
	    return orientation4Exact(a,b,c,d)
	  }
	]

	function slowOrient(args) {
	  var proc = CACHED[args.length]
	  if(!proc) {
	    proc = CACHED[args.length] = orientation(args.length)
	  }
	  return proc.apply(undefined, args)
	}

	function generateOrientationProc() {
	  while(CACHED.length <= NUM_EXPAND) {
	    CACHED.push(orientation(CACHED.length))
	  }
	  var args = []
	  var procArgs = ["slow"]
	  for(var i=0; i<=NUM_EXPAND; ++i) {
	    args.push("a" + i)
	    procArgs.push("o" + i)
	  }
	  var code = [
	    "function getOrientation(", args.join(), "){switch(arguments.length){case 0:case 1:return 0;"
	  ]
	  for(var i=2; i<=NUM_EXPAND; ++i) {
	    code.push("case ", i, ":return o", i, "(", args.slice(0, i).join(), ");")
	  }
	  code.push("}var s=new Array(arguments.length);for(var i=0;i<arguments.length;++i){s[i]=arguments[i]};return slow(s);}return getOrientation")
	  procArgs.push(code.join(""))

	  var proc = Function.apply(undefined, procArgs)
	  module.exports = proc.apply(undefined, [slowOrient].concat(CACHED))
	  for(var i=0; i<=NUM_EXPAND; ++i) {
	    module.exports[i] = CACHED[i]
	  }
	}

	generateOrientationProc()
	});

	var require$$0$20 = (orientation && typeof orientation === 'object' && 'default' in orientation ? orientation['default'] : orientation);

	var orderSegments = createCommonjsModule(function (module) {
	"use strict"

	module.exports = orderSegments

	var orient = require$$0$20

	function horizontalOrder(a, b) {
	  var bl, br
	  if(b[0][0] < b[1][0]) {
	    bl = b[0]
	    br = b[1]
	  } else if(b[0][0] > b[1][0]) {
	    bl = b[1]
	    br = b[0]
	  } else {
	    var alo = Math.min(a[0][1], a[1][1])
	    var ahi = Math.max(a[0][1], a[1][1])
	    var blo = Math.min(b[0][1], b[1][1])
	    var bhi = Math.max(b[0][1], b[1][1])
	    if(ahi < blo) {
	      return ahi - blo
	    }
	    if(alo > bhi) {
	      return alo - bhi
	    }
	    return ahi - bhi
	  }
	  var al, ar
	  if(a[0][1] < a[1][1]) {
	    al = a[0]
	    ar = a[1]
	  } else {
	    al = a[1]
	    ar = a[0]
	  }
	  var d = orient(br, bl, al)
	  if(d) {
	    return d
	  }
	  d = orient(br, bl, ar)
	  if(d) {
	    return d
	  }
	  return ar - br
	}

	function orderSegments(b, a) {
	  var al, ar
	  if(a[0][0] < a[1][0]) {
	    al = a[0]
	    ar = a[1]
	  } else if(a[0][0] > a[1][0]) {
	    al = a[1]
	    ar = a[0]
	  } else {
	    return horizontalOrder(a, b)
	  }
	  var bl, br
	  if(b[0][0] < b[1][0]) {
	    bl = b[0]
	    br = b[1]
	  } else if(b[0][0] > b[1][0]) {
	    bl = b[1]
	    br = b[0]
	  } else {
	    return -horizontalOrder(b, a)
	  }
	  var d1 = orient(al, ar, br)
	  var d2 = orient(al, ar, bl)
	  if(d1 < 0) {
	    if(d2 <= 0) {
	      return d1
	    }
	  } else if(d1 > 0) {
	    if(d2 >= 0) {
	      return d1
	    }
	  } else if(d2) {
	    return d2
	  }
	  d1 = orient(br, bl, ar)
	  d2 = orient(br, bl, al)
	  if(d1 < 0) {
	    if(d2 <= 0) {
	      return d1
	    }
	  } else if(d1 > 0) {
	    if(d2 >= 0) {
	      return d1
	    }
	  } else if(d2) {
	    return d2
	  }
	  return ar[0] - br[0]
	}
	});

	var require$$0$19 = (orderSegments && typeof orderSegments === 'object' && 'default' in orderSegments ? orderSegments['default'] : orderSegments);

	var rbtree = createCommonjsModule(function (module) {
	"use strict"

	module.exports = createRBTree

	var RED   = 0
	var BLACK = 1

	function RBNode(color, key, value, left, right, count) {
	  this._color = color
	  this.key = key
	  this.value = value
	  this.left = left
	  this.right = right
	  this._count = count
	}

	function cloneNode(node) {
	  return new RBNode(node._color, node.key, node.value, node.left, node.right, node._count)
	}

	function repaint(color, node) {
	  return new RBNode(color, node.key, node.value, node.left, node.right, node._count)
	}

	function recount(node) {
	  node._count = 1 + (node.left ? node.left._count : 0) + (node.right ? node.right._count : 0)
	}

	function RedBlackTree(compare, root) {
	  this._compare = compare
	  this.root = root
	}

	var proto = RedBlackTree.prototype

	Object.defineProperty(proto, "keys", {
	  get: function() {
	    var result = []
	    this.forEach(function(k,v) {
	      result.push(k)
	    })
	    return result
	  }
	})

	Object.defineProperty(proto, "values", {
	  get: function() {
	    var result = []
	    this.forEach(function(k,v) {
	      result.push(v)
	    })
	    return result
	  }
	})

	//Returns the number of nodes in the tree
	Object.defineProperty(proto, "length", {
	  get: function() {
	    if(this.root) {
	      return this.root._count
	    }
	    return 0
	  }
	})

	//Insert a new item into the tree
	proto.insert = function(key, value) {
	  var cmp = this._compare
	  //Find point to insert new node at
	  var n = this.root
	  var n_stack = []
	  var d_stack = []
	  while(n) {
	    var d = cmp(key, n.key)
	    n_stack.push(n)
	    d_stack.push(d)
	    if(d <= 0) {
	      n = n.left
	    } else {
	      n = n.right
	    }
	  }
	  //Rebuild path to leaf node
	  n_stack.push(new RBNode(RED, key, value, null, null, 1))
	  for(var s=n_stack.length-2; s>=0; --s) {
	    var n = n_stack[s]
	    if(d_stack[s] <= 0) {
	      n_stack[s] = new RBNode(n._color, n.key, n.value, n_stack[s+1], n.right, n._count+1)
	    } else {
	      n_stack[s] = new RBNode(n._color, n.key, n.value, n.left, n_stack[s+1], n._count+1)
	    }
	  }
	  //Rebalance tree using rotations
	  //console.log("start insert", key, d_stack)
	  for(var s=n_stack.length-1; s>1; --s) {
	    var p = n_stack[s-1]
	    var n = n_stack[s]
	    if(p._color === BLACK || n._color === BLACK) {
	      break
	    }
	    var pp = n_stack[s-2]
	    if(pp.left === p) {
	      if(p.left === n) {
	        var y = pp.right
	        if(y && y._color === RED) {
	          //console.log("LLr")
	          p._color = BLACK
	          pp.right = repaint(BLACK, y)
	          pp._color = RED
	          s -= 1
	        } else {
	          //console.log("LLb")
	          pp._color = RED
	          pp.left = p.right
	          p._color = BLACK
	          p.right = pp
	          n_stack[s-2] = p
	          n_stack[s-1] = n
	          recount(pp)
	          recount(p)
	          if(s >= 3) {
	            var ppp = n_stack[s-3]
	            if(ppp.left === pp) {
	              ppp.left = p
	            } else {
	              ppp.right = p
	            }
	          }
	          break
	        }
	      } else {
	        var y = pp.right
	        if(y && y._color === RED) {
	          //console.log("LRr")
	          p._color = BLACK
	          pp.right = repaint(BLACK, y)
	          pp._color = RED
	          s -= 1
	        } else {
	          //console.log("LRb")
	          p.right = n.left
	          pp._color = RED
	          pp.left = n.right
	          n._color = BLACK
	          n.left = p
	          n.right = pp
	          n_stack[s-2] = n
	          n_stack[s-1] = p
	          recount(pp)
	          recount(p)
	          recount(n)
	          if(s >= 3) {
	            var ppp = n_stack[s-3]
	            if(ppp.left === pp) {
	              ppp.left = n
	            } else {
	              ppp.right = n
	            }
	          }
	          break
	        }
	      }
	    } else {
	      if(p.right === n) {
	        var y = pp.left
	        if(y && y._color === RED) {
	          //console.log("RRr", y.key)
	          p._color = BLACK
	          pp.left = repaint(BLACK, y)
	          pp._color = RED
	          s -= 1
	        } else {
	          //console.log("RRb")
	          pp._color = RED
	          pp.right = p.left
	          p._color = BLACK
	          p.left = pp
	          n_stack[s-2] = p
	          n_stack[s-1] = n
	          recount(pp)
	          recount(p)
	          if(s >= 3) {
	            var ppp = n_stack[s-3]
	            if(ppp.right === pp) {
	              ppp.right = p
	            } else {
	              ppp.left = p
	            }
	          }
	          break
	        }
	      } else {
	        var y = pp.left
	        if(y && y._color === RED) {
	          //console.log("RLr")
	          p._color = BLACK
	          pp.left = repaint(BLACK, y)
	          pp._color = RED
	          s -= 1
	        } else {
	          //console.log("RLb")
	          p.left = n.right
	          pp._color = RED
	          pp.right = n.left
	          n._color = BLACK
	          n.right = p
	          n.left = pp
	          n_stack[s-2] = n
	          n_stack[s-1] = p
	          recount(pp)
	          recount(p)
	          recount(n)
	          if(s >= 3) {
	            var ppp = n_stack[s-3]
	            if(ppp.right === pp) {
	              ppp.right = n
	            } else {
	              ppp.left = n
	            }
	          }
	          break
	        }
	      }
	    }
	  }
	  //Return new tree
	  n_stack[0]._color = BLACK
	  return new RedBlackTree(cmp, n_stack[0])
	}


	//Visit all nodes inorder
	function doVisitFull(visit, node) {
	  if(node.left) {
	    var v = doVisitFull(visit, node.left)
	    if(v) { return v }
	  }
	  var v = visit(node.key, node.value)
	  if(v) { return v }
	  if(node.right) {
	    return doVisitFull(visit, node.right)
	  }
	}

	//Visit half nodes in order
	function doVisitHalf(lo, compare, visit, node) {
	  var l = compare(lo, node.key)
	  if(l <= 0) {
	    if(node.left) {
	      var v = doVisitHalf(lo, compare, visit, node.left)
	      if(v) { return v }
	    }
	    var v = visit(node.key, node.value)
	    if(v) { return v }
	  }
	  if(node.right) {
	    return doVisitHalf(lo, compare, visit, node.right)
	  }
	}

	//Visit all nodes within a range
	function doVisit(lo, hi, compare, visit, node) {
	  var l = compare(lo, node.key)
	  var h = compare(hi, node.key)
	  var v
	  if(l <= 0) {
	    if(node.left) {
	      v = doVisit(lo, hi, compare, visit, node.left)
	      if(v) { return v }
	    }
	    if(h > 0) {
	      v = visit(node.key, node.value)
	      if(v) { return v }
	    }
	  }
	  if(h > 0 && node.right) {
	    return doVisit(lo, hi, compare, visit, node.right)
	  }
	}


	proto.forEach = function rbTreeForEach(visit, lo, hi) {
	  if(!this.root) {
	    return
	  }
	  switch(arguments.length) {
	    case 1:
	      return doVisitFull(visit, this.root)
	    break

	    case 2:
	      return doVisitHalf(lo, this._compare, visit, this.root)
	    break

	    case 3:
	      if(this._compare(lo, hi) >= 0) {
	        return
	      }
	      return doVisit(lo, hi, this._compare, visit, this.root)
	    break
	  }
	}

	//First item in list
	Object.defineProperty(proto, "begin", {
	  get: function() {
	    var stack = []
	    var n = this.root
	    while(n) {
	      stack.push(n)
	      n = n.left
	    }
	    return new RedBlackTreeIterator(this, stack)
	  }
	})

	//Last item in list
	Object.defineProperty(proto, "end", {
	  get: function() {
	    var stack = []
	    var n = this.root
	    while(n) {
	      stack.push(n)
	      n = n.right
	    }
	    return new RedBlackTreeIterator(this, stack)
	  }
	})

	//Find the ith item in the tree
	proto.at = function(idx) {
	  if(idx < 0) {
	    return new RedBlackTreeIterator(this, [])
	  }
	  var n = this.root
	  var stack = []
	  while(true) {
	    stack.push(n)
	    if(n.left) {
	      if(idx < n.left._count) {
	        n = n.left
	        continue
	      }
	      idx -= n.left._count
	    }
	    if(!idx) {
	      return new RedBlackTreeIterator(this, stack)
	    }
	    idx -= 1
	    if(n.right) {
	      if(idx >= n.right._count) {
	        break
	      }
	      n = n.right
	    } else {
	      break
	    }
	  }
	  return new RedBlackTreeIterator(this, [])
	}

	proto.ge = function(key) {
	  var cmp = this._compare
	  var n = this.root
	  var stack = []
	  var last_ptr = 0
	  while(n) {
	    var d = cmp(key, n.key)
	    stack.push(n)
	    if(d <= 0) {
	      last_ptr = stack.length
	    }
	    if(d <= 0) {
	      n = n.left
	    } else {
	      n = n.right
	    }
	  }
	  stack.length = last_ptr
	  return new RedBlackTreeIterator(this, stack)
	}

	proto.gt = function(key) {
	  var cmp = this._compare
	  var n = this.root
	  var stack = []
	  var last_ptr = 0
	  while(n) {
	    var d = cmp(key, n.key)
	    stack.push(n)
	    if(d < 0) {
	      last_ptr = stack.length
	    }
	    if(d < 0) {
	      n = n.left
	    } else {
	      n = n.right
	    }
	  }
	  stack.length = last_ptr
	  return new RedBlackTreeIterator(this, stack)
	}

	proto.lt = function(key) {
	  var cmp = this._compare
	  var n = this.root
	  var stack = []
	  var last_ptr = 0
	  while(n) {
	    var d = cmp(key, n.key)
	    stack.push(n)
	    if(d > 0) {
	      last_ptr = stack.length
	    }
	    if(d <= 0) {
	      n = n.left
	    } else {
	      n = n.right
	    }
	  }
	  stack.length = last_ptr
	  return new RedBlackTreeIterator(this, stack)
	}

	proto.le = function(key) {
	  var cmp = this._compare
	  var n = this.root
	  var stack = []
	  var last_ptr = 0
	  while(n) {
	    var d = cmp(key, n.key)
	    stack.push(n)
	    if(d >= 0) {
	      last_ptr = stack.length
	    }
	    if(d < 0) {
	      n = n.left
	    } else {
	      n = n.right
	    }
	  }
	  stack.length = last_ptr
	  return new RedBlackTreeIterator(this, stack)
	}

	//Finds the item with key if it exists
	proto.find = function(key) {
	  var cmp = this._compare
	  var n = this.root
	  var stack = []
	  while(n) {
	    var d = cmp(key, n.key)
	    stack.push(n)
	    if(d === 0) {
	      return new RedBlackTreeIterator(this, stack)
	    }
	    if(d <= 0) {
	      n = n.left
	    } else {
	      n = n.right
	    }
	  }
	  return new RedBlackTreeIterator(this, [])
	}

	//Removes item with key from tree
	proto.remove = function(key) {
	  var iter = this.find(key)
	  if(iter) {
	    return iter.remove()
	  }
	  return this
	}

	//Returns the item at `key`
	proto.get = function(key) {
	  var cmp = this._compare
	  var n = this.root
	  while(n) {
	    var d = cmp(key, n.key)
	    if(d === 0) {
	      return n.value
	    }
	    if(d <= 0) {
	      n = n.left
	    } else {
	      n = n.right
	    }
	  }
	  return
	}

	//Iterator for red black tree
	function RedBlackTreeIterator(tree, stack) {
	  this.tree = tree
	  this._stack = stack
	}

	var iproto = RedBlackTreeIterator.prototype

	//Test if iterator is valid
	Object.defineProperty(iproto, "valid", {
	  get: function() {
	    return this._stack.length > 0
	  }
	})

	//Node of the iterator
	Object.defineProperty(iproto, "node", {
	  get: function() {
	    if(this._stack.length > 0) {
	      return this._stack[this._stack.length-1]
	    }
	    return null
	  },
	  enumerable: true
	})

	//Makes a copy of an iterator
	iproto.clone = function() {
	  return new RedBlackTreeIterator(this.tree, this._stack.slice())
	}

	//Swaps two nodes
	function swapNode(n, v) {
	  n.key = v.key
	  n.value = v.value
	  n.left = v.left
	  n.right = v.right
	  n._color = v._color
	  n._count = v._count
	}

	//Fix up a double black node in a tree
	function fixDoubleBlack(stack) {
	  var n, p, s, z
	  for(var i=stack.length-1; i>=0; --i) {
	    n = stack[i]
	    if(i === 0) {
	      n._color = BLACK
	      return
	    }
	    //console.log("visit node:", n.key, i, stack[i].key, stack[i-1].key)
	    p = stack[i-1]
	    if(p.left === n) {
	      //console.log("left child")
	      s = p.right
	      if(s.right && s.right._color === RED) {
	        //console.log("case 1: right sibling child red")
	        s = p.right = cloneNode(s)
	        z = s.right = cloneNode(s.right)
	        p.right = s.left
	        s.left = p
	        s.right = z
	        s._color = p._color
	        n._color = BLACK
	        p._color = BLACK
	        z._color = BLACK
	        recount(p)
	        recount(s)
	        if(i > 1) {
	          var pp = stack[i-2]
	          if(pp.left === p) {
	            pp.left = s
	          } else {
	            pp.right = s
	          }
	        }
	        stack[i-1] = s
	        return
	      } else if(s.left && s.left._color === RED) {
	        //console.log("case 1: left sibling child red")
	        s = p.right = cloneNode(s)
	        z = s.left = cloneNode(s.left)
	        p.right = z.left
	        s.left = z.right
	        z.left = p
	        z.right = s
	        z._color = p._color
	        p._color = BLACK
	        s._color = BLACK
	        n._color = BLACK
	        recount(p)
	        recount(s)
	        recount(z)
	        if(i > 1) {
	          var pp = stack[i-2]
	          if(pp.left === p) {
	            pp.left = z
	          } else {
	            pp.right = z
	          }
	        }
	        stack[i-1] = z
	        return
	      }
	      if(s._color === BLACK) {
	        if(p._color === RED) {
	          //console.log("case 2: black sibling, red parent", p.right.value)
	          p._color = BLACK
	          p.right = repaint(RED, s)
	          return
	        } else {
	          //console.log("case 2: black sibling, black parent", p.right.value)
	          p.right = repaint(RED, s)
	          continue  
	        }
	      } else {
	        //console.log("case 3: red sibling")
	        s = cloneNode(s)
	        p.right = s.left
	        s.left = p
	        s._color = p._color
	        p._color = RED
	        recount(p)
	        recount(s)
	        if(i > 1) {
	          var pp = stack[i-2]
	          if(pp.left === p) {
	            pp.left = s
	          } else {
	            pp.right = s
	          }
	        }
	        stack[i-1] = s
	        stack[i] = p
	        if(i+1 < stack.length) {
	          stack[i+1] = n
	        } else {
	          stack.push(n)
	        }
	        i = i+2
	      }
	    } else {
	      //console.log("right child")
	      s = p.left
	      if(s.left && s.left._color === RED) {
	        //console.log("case 1: left sibling child red", p.value, p._color)
	        s = p.left = cloneNode(s)
	        z = s.left = cloneNode(s.left)
	        p.left = s.right
	        s.right = p
	        s.left = z
	        s._color = p._color
	        n._color = BLACK
	        p._color = BLACK
	        z._color = BLACK
	        recount(p)
	        recount(s)
	        if(i > 1) {
	          var pp = stack[i-2]
	          if(pp.right === p) {
	            pp.right = s
	          } else {
	            pp.left = s
	          }
	        }
	        stack[i-1] = s
	        return
	      } else if(s.right && s.right._color === RED) {
	        //console.log("case 1: right sibling child red")
	        s = p.left = cloneNode(s)
	        z = s.right = cloneNode(s.right)
	        p.left = z.right
	        s.right = z.left
	        z.right = p
	        z.left = s
	        z._color = p._color
	        p._color = BLACK
	        s._color = BLACK
	        n._color = BLACK
	        recount(p)
	        recount(s)
	        recount(z)
	        if(i > 1) {
	          var pp = stack[i-2]
	          if(pp.right === p) {
	            pp.right = z
	          } else {
	            pp.left = z
	          }
	        }
	        stack[i-1] = z
	        return
	      }
	      if(s._color === BLACK) {
	        if(p._color === RED) {
	          //console.log("case 2: black sibling, red parent")
	          p._color = BLACK
	          p.left = repaint(RED, s)
	          return
	        } else {
	          //console.log("case 2: black sibling, black parent")
	          p.left = repaint(RED, s)
	          continue  
	        }
	      } else {
	        //console.log("case 3: red sibling")
	        s = cloneNode(s)
	        p.left = s.right
	        s.right = p
	        s._color = p._color
	        p._color = RED
	        recount(p)
	        recount(s)
	        if(i > 1) {
	          var pp = stack[i-2]
	          if(pp.right === p) {
	            pp.right = s
	          } else {
	            pp.left = s
	          }
	        }
	        stack[i-1] = s
	        stack[i] = p
	        if(i+1 < stack.length) {
	          stack[i+1] = n
	        } else {
	          stack.push(n)
	        }
	        i = i+2
	      }
	    }
	  }
	}

	//Removes item at iterator from tree
	iproto.remove = function() {
	  var stack = this._stack
	  if(stack.length === 0) {
	    return this.tree
	  }
	  //First copy path to node
	  var cstack = new Array(stack.length)
	  var n = stack[stack.length-1]
	  cstack[cstack.length-1] = new RBNode(n._color, n.key, n.value, n.left, n.right, n._count)
	  for(var i=stack.length-2; i>=0; --i) {
	    var n = stack[i]
	    if(n.left === stack[i+1]) {
	      cstack[i] = new RBNode(n._color, n.key, n.value, cstack[i+1], n.right, n._count)
	    } else {
	      cstack[i] = new RBNode(n._color, n.key, n.value, n.left, cstack[i+1], n._count)
	    }
	  }

	  //Get node
	  n = cstack[cstack.length-1]
	  //console.log("start remove: ", n.value)

	  //If not leaf, then swap with previous node
	  if(n.left && n.right) {
	    //console.log("moving to leaf")

	    //First walk to previous leaf
	    var split = cstack.length
	    n = n.left
	    while(n.right) {
	      cstack.push(n)
	      n = n.right
	    }
	    //Copy path to leaf
	    var v = cstack[split-1]
	    cstack.push(new RBNode(n._color, v.key, v.value, n.left, n.right, n._count))
	    cstack[split-1].key = n.key
	    cstack[split-1].value = n.value

	    //Fix up stack
	    for(var i=cstack.length-2; i>=split; --i) {
	      n = cstack[i]
	      cstack[i] = new RBNode(n._color, n.key, n.value, n.left, cstack[i+1], n._count)
	    }
	    cstack[split-1].left = cstack[split]
	  }
	  //console.log("stack=", cstack.map(function(v) { return v.value }))

	  //Remove leaf node
	  n = cstack[cstack.length-1]
	  if(n._color === RED) {
	    //Easy case: removing red leaf
	    //console.log("RED leaf")
	    var p = cstack[cstack.length-2]
	    if(p.left === n) {
	      p.left = null
	    } else if(p.right === n) {
	      p.right = null
	    }
	    cstack.pop()
	    for(var i=0; i<cstack.length; ++i) {
	      cstack[i]._count--
	    }
	    return new RedBlackTree(this.tree._compare, cstack[0])
	  } else {
	    if(n.left || n.right) {
	      //Second easy case:  Single child black parent
	      //console.log("BLACK single child")
	      if(n.left) {
	        swapNode(n, n.left)
	      } else if(n.right) {
	        swapNode(n, n.right)
	      }
	      //Child must be red, so repaint it black to balance color
	      n._color = BLACK
	      for(var i=0; i<cstack.length-1; ++i) {
	        cstack[i]._count--
	      }
	      return new RedBlackTree(this.tree._compare, cstack[0])
	    } else if(cstack.length === 1) {
	      //Third easy case: root
	      //console.log("ROOT")
	      return new RedBlackTree(this.tree._compare, null)
	    } else {
	      //Hard case: Repaint n, and then do some nasty stuff
	      //console.log("BLACK leaf no children")
	      for(var i=0; i<cstack.length; ++i) {
	        cstack[i]._count--
	      }
	      var parent = cstack[cstack.length-2]
	      fixDoubleBlack(cstack)
	      //Fix up links
	      if(parent.left === n) {
	        parent.left = null
	      } else {
	        parent.right = null
	      }
	    }
	  }
	  return new RedBlackTree(this.tree._compare, cstack[0])
	}

	//Returns key
	Object.defineProperty(iproto, "key", {
	  get: function() {
	    if(this._stack.length > 0) {
	      return this._stack[this._stack.length-1].key
	    }
	    return
	  },
	  enumerable: true
	})

	//Returns value
	Object.defineProperty(iproto, "value", {
	  get: function() {
	    if(this._stack.length > 0) {
	      return this._stack[this._stack.length-1].value
	    }
	    return
	  },
	  enumerable: true
	})


	//Returns the position of this iterator in the sorted list
	Object.defineProperty(iproto, "index", {
	  get: function() {
	    var idx = 0
	    var stack = this._stack
	    if(stack.length === 0) {
	      var r = this.tree.root
	      if(r) {
	        return r._count
	      }
	      return 0
	    } else if(stack[stack.length-1].left) {
	      idx = stack[stack.length-1].left._count
	    }
	    for(var s=stack.length-2; s>=0; --s) {
	      if(stack[s+1] === stack[s].right) {
	        ++idx
	        if(stack[s].left) {
	          idx += stack[s].left._count
	        }
	      }
	    }
	    return idx
	  },
	  enumerable: true
	})

	//Advances iterator to next element in list
	iproto.next = function() {
	  var stack = this._stack
	  if(stack.length === 0) {
	    return
	  }
	  var n = stack[stack.length-1]
	  if(n.right) {
	    n = n.right
	    while(n) {
	      stack.push(n)
	      n = n.left
	    }
	  } else {
	    stack.pop()
	    while(stack.length > 0 && stack[stack.length-1].right === n) {
	      n = stack[stack.length-1]
	      stack.pop()
	    }
	  }
	}

	//Checks if iterator is at end of tree
	Object.defineProperty(iproto, "hasNext", {
	  get: function() {
	    var stack = this._stack
	    if(stack.length === 0) {
	      return false
	    }
	    if(stack[stack.length-1].right) {
	      return true
	    }
	    for(var s=stack.length-1; s>0; --s) {
	      if(stack[s-1].left === stack[s]) {
	        return true
	      }
	    }
	    return false
	  }
	})

	//Update value
	iproto.update = function(value) {
	  var stack = this._stack
	  if(stack.length === 0) {
	    throw new Error("Can't update empty node!")
	  }
	  var cstack = new Array(stack.length)
	  var n = stack[stack.length-1]
	  cstack[cstack.length-1] = new RBNode(n._color, n.key, value, n.left, n.right, n._count)
	  for(var i=stack.length-2; i>=0; --i) {
	    n = stack[i]
	    if(n.left === stack[i+1]) {
	      cstack[i] = new RBNode(n._color, n.key, n.value, cstack[i+1], n.right, n._count)
	    } else {
	      cstack[i] = new RBNode(n._color, n.key, n.value, n.left, cstack[i+1], n._count)
	    }
	  }
	  return new RedBlackTree(this.tree._compare, cstack[0])
	}

	//Moves iterator backward one element
	iproto.prev = function() {
	  var stack = this._stack
	  if(stack.length === 0) {
	    return
	  }
	  var n = stack[stack.length-1]
	  if(n.left) {
	    n = n.left
	    while(n) {
	      stack.push(n)
	      n = n.right
	    }
	  } else {
	    stack.pop()
	    while(stack.length > 0 && stack[stack.length-1].left === n) {
	      n = stack[stack.length-1]
	      stack.pop()
	    }
	  }
	}

	//Checks if iterator is at start of tree
	Object.defineProperty(iproto, "hasPrev", {
	  get: function() {
	    var stack = this._stack
	    if(stack.length === 0) {
	      return false
	    }
	    if(stack[stack.length-1].left) {
	      return true
	    }
	    for(var s=stack.length-1; s>0; --s) {
	      if(stack[s-1].right === stack[s]) {
	        return true
	      }
	    }
	    return false
	  }
	})

	//Default comparison function
	function defaultCompare(a, b) {
	  if(a < b) {
	    return -1
	  }
	  if(a > b) {
	    return 1
	  }
	  return 0
	}

	//Build a tree
	function createRBTree(compare) {
	  return new RedBlackTree(compare || defaultCompare, null)
	}
	});

	var require$$2$8 = (rbtree && typeof rbtree === 'object' && 'default' in rbtree ? rbtree['default'] : rbtree);

	var slabs = createCommonjsModule(function (module) {
	"use strict"

	module.exports = createSlabDecomposition

	var bounds = require$$3$8
	var createRBTree = require$$2$8
	var orient = require$$0$20
	var orderSegments = require$$0$19

	function SlabDecomposition(slabs, coordinates, horizontal) {
	  this.slabs = slabs
	  this.coordinates = coordinates
	  this.horizontal = horizontal
	}

	var proto = SlabDecomposition.prototype

	function compareHorizontal(e, y) {
	  return e.y - y
	}

	function searchBucket(root, p) {
	  var lastNode = null
	  while(root) {
	    var seg = root.key
	    var l, r
	    if(seg[0][0] < seg[1][0]) {
	      l = seg[0]
	      r = seg[1]
	    } else {
	      l = seg[1]
	      r = seg[0]
	    }
	    var o = orient(l, r, p)
	    if(o < 0) {
	      root = root.left
	    } else if(o > 0) {
	      if(p[0] !== seg[1][0]) {
	        lastNode = root
	        root = root.right
	      } else {
	        var val = searchBucket(root.right, p)
	        if(val) {
	          return val
	        }
	        root = root.left
	      }
	    } else {
	      if(p[0] !== seg[1][0]) {
	        return root
	      } else {
	        var val = searchBucket(root.right, p)
	        if(val) {
	          return val
	        }
	        root = root.left
	      }
	    }
	  }
	  return lastNode
	}

	proto.castUp = function(p) {
	  var bucket = bounds.le(this.coordinates, p[0])
	  if(bucket < 0) {
	    return -1
	  }
	  var root = this.slabs[bucket]
	  var hitNode = searchBucket(this.slabs[bucket], p)
	  var lastHit = -1
	  if(hitNode) {
	    lastHit = hitNode.value
	  }
	  //Edge case: need to handle horizontal segments (sucks)
	  if(this.coordinates[bucket] === p[0]) {
	    var lastSegment = null
	    if(hitNode) {
	      lastSegment = hitNode.key
	    }
	    if(bucket > 0) {
	      var otherHitNode = searchBucket(this.slabs[bucket-1], p)
	      if(otherHitNode) {
	        if(lastSegment) {
	          if(orderSegments(otherHitNode.key, lastSegment) > 0) {
	            lastSegment = otherHitNode.key
	            lastHit = otherHitNode.value
	          }
	        } else {
	          lastHit = otherHitNode.value
	          lastSegment = otherHitNode.key
	        }
	      }
	    }
	    var horiz = this.horizontal[bucket]
	    if(horiz.length > 0) {
	      var hbucket = bounds.ge(horiz, p[1], compareHorizontal)
	      if(hbucket < horiz.length) {
	        var e = horiz[hbucket]
	        if(p[1] === e.y) {
	          if(e.closed) {
	            return e.index
	          } else {
	            while(hbucket < horiz.length-1 && horiz[hbucket+1].y === p[1]) {
	              hbucket = hbucket+1
	              e = horiz[hbucket]
	              if(e.closed) {
	                return e.index
	              }
	            }
	            if(e.y === p[1] && !e.start) {
	              hbucket = hbucket+1
	              if(hbucket >= horiz.length) {
	                return lastHit
	              }
	              e = horiz[hbucket]
	            }
	          }
	        }
	        //Check if e is above/below last segment
	        if(e.start) {
	          if(lastSegment) {
	            var o = orient(lastSegment[0], lastSegment[1], [p[0], e.y])
	            if(lastSegment[0][0] > lastSegment[1][0]) {
	              o = -o
	            }
	            if(o > 0) {
	              lastHit = e.index
	            }
	          } else {
	            lastHit = e.index
	          }
	        } else if(e.y !== p[1]) {
	          lastHit = e.index
	        }
	      }
	    }
	  }
	  return lastHit
	}

	function IntervalSegment(y, index, start, closed) {
	  this.y = y
	  this.index = index
	  this.start = start
	  this.closed = closed
	}

	function Event(x, segment, create, index) {
	  this.x = x
	  this.segment = segment
	  this.create = create
	  this.index = index
	}


	function createSlabDecomposition(segments) {
	  var numSegments = segments.length
	  var numEvents = 2 * numSegments
	  var events = new Array(numEvents)
	  for(var i=0; i<numSegments; ++i) {
	    var s = segments[i]
	    var f = s[0][0] < s[1][0]
	    events[2*i] = new Event(s[0][0], s, f, i)
	    events[2*i+1] = new Event(s[1][0], s, !f, i)
	  }
	  events.sort(function(a,b) {
	    var d = a.x - b.x
	    if(d) {
	      return d
	    }
	    d = a.create - b.create
	    if(d) {
	      return d
	    }
	    return Math.min(a.segment[0][1], a.segment[1][1]) - Math.min(b.segment[0][1], b.segment[1][1])
	  })
	  var tree = createRBTree(orderSegments)
	  var slabs = []
	  var lines = []
	  var horizontal = []
	  var lastX = -Infinity
	  for(var i=0; i<numEvents; ) {
	    var x = events[i].x
	    var horiz = []
	    while(i < numEvents) {
	      var e = events[i]
	      if(e.x !== x) {
	        break
	      }
	      i += 1
	      if(e.segment[0][0] === e.x && e.segment[1][0] === e.x) {
	        if(e.create) {
	          if(e.segment[0][1] < e.segment[1][1]) {
	            horiz.push(new IntervalSegment(
	                e.segment[0][1],
	                e.index,
	                true,
	                true))
	            horiz.push(new IntervalSegment(
	                e.segment[1][1],
	                e.index,
	                false,
	                false))
	          } else {
	            horiz.push(new IntervalSegment(
	                e.segment[1][1],
	                e.index,
	                true,
	                false))
	            horiz.push(new IntervalSegment(
	                e.segment[0][1],
	                e.index,
	                false,
	                true))
	          }
	        }
	      } else {
	        if(e.create) {
	          tree = tree.insert(e.segment, e.index)
	        } else {
	          tree = tree.remove(e.segment)
	        }
	      }
	    }
	    slabs.push(tree.root)
	    lines.push(x)
	    horizontal.push(horiz)
	  }
	  return new SlabDecomposition(slabs, lines, horizontal)
	}
	});

	var require$$2$6 = (slabs && typeof slabs === 'object' && 'default' in slabs ? slabs['default'] : slabs);

	var pnpBig = createCommonjsModule(function (module) {
	module.exports = preprocessPolygon

	var orient = require$$0$20[3]
	var makeSlabs = require$$2$6
	var makeIntervalTree = require$$1$13
	var bsearch = require$$3$8

	function visitInterval() {
	  return true
	}

	function intervalSearch(table) {
	  return function(x, y) {
	    var tree = table[x]
	    if(tree) {
	      return !!tree.queryPoint(y, visitInterval)
	    }
	    return false
	  }
	}

	function buildVerticalIndex(segments) {
	  var table = {}
	  for(var i=0; i<segments.length; ++i) {
	    var s = segments[i]
	    var x = s[0][0]
	    var y0 = s[0][1]
	    var y1 = s[1][1]
	    var p = [ Math.min(y0, y1), Math.max(y0, y1) ]
	    if(x in table) {
	      table[x].push(p)
	    } else {
	      table[x] = [ p ]
	    }
	  }
	  var intervalTable = {}
	  var keys = Object.keys(table)
	  for(var i=0; i<keys.length; ++i) {
	    var segs = table[keys[i]]
	    intervalTable[keys[i]] = makeIntervalTree(segs)
	  }
	  return intervalSearch(intervalTable)
	}

	function buildSlabSearch(slabs, coordinates) {
	  return function(p) {
	    var bucket = bsearch.le(coordinates, p[0])
	    if(bucket < 0) {
	      return 1
	    }
	    var root = slabs[bucket]
	    if(!root) {
	      if(bucket > 0 && coordinates[bucket] === p[0]) {
	        root = slabs[bucket-1]
	      } else {
	        return 1
	      }
	    }
	    var lastOrientation = 1
	    while(root) {
	      var s = root.key
	      var o = orient(p, s[0], s[1])
	      if(s[0][0] < s[1][0]) {
	        if(o < 0) {
	          root = root.left
	        } else if(o > 0) {
	          lastOrientation = -1
	          root = root.right
	        } else {
	          return 0
	        }
	      } else {
	        if(o > 0) {
	          root = root.left
	        } else if(o < 0) {
	          lastOrientation = 1
	          root = root.right
	        } else {
	          return 0
	        }
	      }
	    }
	    return lastOrientation
	  }
	}

	function classifyEmpty(p) {
	  return 1
	}

	function createClassifyVertical(testVertical) {
	  return function classify(p) {
	    if(testVertical(p[0], p[1])) {
	      return 0
	    }
	    return 1
	  }
	}

	function createClassifyPointDegen(testVertical, testNormal) {
	  return function classify(p) {
	    if(testVertical(p[0], p[1])) {
	      return 0
	    }
	    return testNormal(p)
	  }
	}

	function preprocessPolygon(loops) {
	  //Compute number of loops
	  var numLoops = loops.length

	  //Unpack segments
	  var segments = []
	  var vsegments = []
	  var ptr = 0
	  for(var i=0; i<numLoops; ++i) {
	    var loop = loops[i]
	    var numVertices = loop.length
	    for(var s=numVertices-1,t=0; t<numVertices; s=(t++)) {
	      var a = loop[s]
	      var b = loop[t]
	      if(a[0] === b[0]) {
	        vsegments.push([a,b])
	      } else {
	        segments.push([a,b])
	      }
	    }
	  }

	  //Degenerate case: All loops are empty
	  if(segments.length === 0) {
	    if(vsegments.length === 0) {
	      return classifyEmpty
	    } else {
	      return createClassifyVertical(buildVerticalIndex(vsegments))
	    }
	  }

	  //Build slab decomposition
	  var slabs = makeSlabs(segments)
	  var testSlab = buildSlabSearch(slabs.slabs, slabs.coordinates)

	  if(vsegments.length === 0) {
	    return testSlab
	  } else {
	    return createClassifyPointDegen(
	      buildVerticalIndex(vsegments),
	      testSlab)
	  }
	}
	});

	var processPolygon = (pnpBig && typeof pnpBig === 'object' && 'default' in pnpBig ? pnpBig['default'] : pnpBig);

	var spherical = createCommonjsModule(function (module, exports) {
	var π = Math.PI,
	    π_4 = π / 4,
	    radians = π / 180;

	exports.name = "spherical";
	exports.formatDistance = formatDistance;
	exports.ringArea = ringArea;
	exports.absoluteArea = absoluteArea;
	exports.triangleArea = triangleArea;
	exports.distance = haversinDistance; // XXX why two implementations?

	function formatDistance(k) {
	  var km = k * radians * 6371;
	  return (km > 1 ? km.toFixed(3) + "km" : (km * 1000).toPrecision(3) + "m") + " (" + k.toPrecision(3) + "°)";
	}

	function ringArea(ring) {
	  if (!ring.length) return 0;
	  var area = 0,
	      p = ring[0],
	      λ = p[0] * radians,
	      φ = p[1] * radians / 2 + π_4,
	      λ0 = λ,
	      cosφ0 = Math.cos(φ),
	      sinφ0 = Math.sin(φ);

	  for (var i = 1, n = ring.length; i < n; ++i) {
	    p = ring[i], λ = p[0] * radians, φ = p[1] * radians / 2 + π_4;

	    // Spherical excess E for a spherical triangle with vertices: south pole,
	    // previous point, current point.  Uses a formula derived from Cagnoli’s
	    // theorem.  See Todhunter, Spherical Trig. (1871), Sec. 103, Eq. (2).
	    var dλ = λ - λ0,
	        cosφ = Math.cos(φ),
	        sinφ = Math.sin(φ),
	        k = sinφ0 * sinφ,
	        u = cosφ0 * cosφ + k * Math.cos(dλ),
	        v = k * Math.sin(dλ);
	    area += Math.atan2(v, u);

	    // Advance the previous point.
	    λ0 = λ, cosφ0 = cosφ, sinφ0 = sinφ;
	  }

	  return 2 * (area > π ? area - 2 * π : area < -π ? area + 2 * π : area);
	}

	function absoluteArea(a) {
	  return a < 0 ? a + 4 * π : a;
	}

	function triangleArea(t) {
	  var a = distance(t[0], t[1]),
	      b = distance(t[1], t[2]),
	      c = distance(t[2], t[0]),
	      s = (a + b + c) / 2;
	  return 4 * Math.atan(Math.sqrt(Math.max(0, Math.tan(s / 2) * Math.tan((s - a) / 2) * Math.tan((s - b) / 2) * Math.tan((s - c) / 2))));
	}

	function distance(a, b) {
	  var Δλ = (b[0] - a[0]) * radians,
	      sinΔλ = Math.sin(Δλ),
	      cosΔλ = Math.cos(Δλ),
	      sinφ0 = Math.sin(a[1] * radians),
	      cosφ0 = Math.cos(a[1] * radians),
	      sinφ1 = Math.sin(b[1] * radians),
	      cosφ1 = Math.cos(b[1] * radians),
	      _;
	  return Math.atan2(Math.sqrt((_ = cosφ1 * sinΔλ) * _ + (_ = cosφ0 * sinφ1 - sinφ0 * cosφ1 * cosΔλ) * _), sinφ0 * sinφ1 + cosφ0 * cosφ1 * cosΔλ);
	}

	function haversinDistance(x0, y0, x1, y1) {
	  x0 *= radians, y0 *= radians, x1 *= radians, y1 *= radians;
	  return 2 * Math.asin(Math.sqrt(haversin(y1 - y0) + Math.cos(y0) * Math.cos(y1) * haversin(x1 - x0)));
	}

	function haversin(x) {
	  return (x = Math.sin(x / 2)) * x;
	}
	});

	var ringArea = spherical.ringArea;

	var cartesian = createCommonjsModule(function (module, exports) {
	exports.name = "cartesian";
	exports.formatDistance = formatDistance;
	exports.ringArea = ringArea;
	exports.absoluteArea = Math.abs;
	exports.triangleArea = triangleArea;
	exports.distance = distance;

	function formatDistance(d) {
	  return d.toString();
	}

	function ringArea(ring) {
	  var i = -1,
	      n = ring.length,
	      a,
	      b = ring[n - 1],
	      area = 0;

	  while (++i < n) {
	    a = b;
	    b = ring[i];
	    area += a[0] * b[1] - a[1] * b[0];
	  }

	  return area * .5;
	}

	function triangleArea(triangle) {
	  return Math.abs(
	    (triangle[0][0] - triangle[2][0]) * (triangle[1][1] - triangle[0][1])
	    - (triangle[0][0] - triangle[1][0]) * (triangle[2][1] - triangle[0][1])
	  );
	}

	function distance(x0, y0, x1, y1) {
	  var dx = x0 - x1, dy = y0 - y1;
	  return Math.sqrt(dx * dx + dy * dy);
	}
	});

	var ringArea$1 = cartesian.ringArea;

	/**
	 * Modifies the point order of the given polygon rings such that the first ring is ordered
	 * clockwise and all others anti-clockwise. Modification happens in-place.
	 *
	 * @param {Array} rings - Polygon rings to reorder (in-place)
	 * @param {boolean} [isCartesian=false] - whether coordinates are cartesian or spherical degrees
	 */
	function ensureClockwisePolygon(rings) {
	  var isCartesian = arguments.length <= 1 || arguments[1] === undefined ? false : arguments[1];

	  // first ring = exterior, clockwise
	  // other rings = interior, anti-clockwise
	  var ringAreaFn = isCartesian ? ringArea$1 : ringArea;
	  for (var i = 0; i < rings.length; i++) {
	    var area = ringAreaFn(rings[i]);
	    if (i === 0 && area < 0 || i > 0 && area > 0) {
	      rings[i].reverse();
	    }
	  }
	}

	/**
	 * Preprocesses an array of polygons to answer the point-in-polygon question efficiently.
	 *
	 * @param {Array} polygons - A list of polygons where the exterior ring of each polygon is in clockwise and the interior rings in anti-clockwise order.
	 * @return {function} A function classify(point) which returns the index of the first found polygon containing point, or -1 if not in any polygon.
	 */
	function getPointInPolygonsFn(polygons) {
	  var classifiers = polygons.map(processPolygon);
	  var npolys = polygons.length;

	  return function (point) {
	    for (var i = 0; i < npolys; i++) {
	      if (classifiers[i](point) <= 0) {
	        return i;
	      }
	    }
	    return -1;
	  };
	}

	var index$3 = createCommonjsModule(function (module) {
	/**
	 * Determine if an object is Buffer
	 *
	 * Author:   Feross Aboukhadijeh <feross@feross.org> <http://feross.org>
	 * License:  MIT
	 *
	 * `npm install is-buffer`
	 */

	module.exports = function (obj) {
	  return !!(obj != null &&
	    (obj._isBuffer || // For Safari 5-7 (missing Object.prototype.constructor)
	      (obj.constructor &&
	      typeof obj.constructor.isBuffer === 'function' &&
	      obj.constructor.isBuffer(obj))
	    ))
	}
	});

	var require$$0$23 = (index$3 && typeof index$3 === 'object' && 'default' in index$3 ? index$3['default'] : index$3);

	var iota = createCommonjsModule(function (module) {
	"use strict"

	function iota(n) {
	  var result = new Array(n)
	  for(var i=0; i<n; ++i) {
	    result[i] = i
	  }
	  return result
	}

	module.exports = iota
	});

	var require$$1$16 = (iota && typeof iota === 'object' && 'default' in iota ? iota['default'] : iota);

	var ndarray = createCommonjsModule(function (module) {
	var iota = require$$1$16
	var isBuffer = require$$0$23

	var hasTypedArrays  = ((typeof Float64Array) !== "undefined")

	function compare1st(a, b) {
	  return a[0] - b[0]
	}

	function order() {
	  var stride = this.stride
	  var terms = new Array(stride.length)
	  var i
	  for(i=0; i<terms.length; ++i) {
	    terms[i] = [Math.abs(stride[i]), i]
	  }
	  terms.sort(compare1st)
	  var result = new Array(terms.length)
	  for(i=0; i<result.length; ++i) {
	    result[i] = terms[i][1]
	  }
	  return result
	}

	function compileConstructor(dtype, dimension) {
	  var className = ["View", dimension, "d", dtype].join("")
	  if(dimension < 0) {
	    className = "View_Nil" + dtype
	  }
	  var useGetters = (dtype === "generic")

	  if(dimension === -1) {
	    //Special case for trivial arrays
	    var code =
	      "function "+className+"(a){this.data=a;};\
var proto="+className+".prototype;\
proto.dtype='"+dtype+"';\
proto.index=function(){return -1};\
proto.size=0;\
proto.dimension=-1;\
proto.shape=proto.stride=proto.order=[];\
proto.lo=proto.hi=proto.transpose=proto.step=\
function(){return new "+className+"(this.data);};\
proto.get=proto.set=function(){};\
proto.pick=function(){return null};\
return function construct_"+className+"(a){return new "+className+"(a);}"
	    var procedure = new Function(code)
	    return procedure()
	  } else if(dimension === 0) {
	    //Special case for 0d arrays
	    var code =
	      "function "+className+"(a,d) {\
this.data = a;\
this.offset = d\
};\
var proto="+className+".prototype;\
proto.dtype='"+dtype+"';\
proto.index=function(){return this.offset};\
proto.dimension=0;\
proto.size=1;\
proto.shape=\
proto.stride=\
proto.order=[];\
proto.lo=\
proto.hi=\
proto.transpose=\
proto.step=function "+className+"_copy() {\
return new "+className+"(this.data,this.offset)\
};\
proto.pick=function "+className+"_pick(){\
return TrivialArray(this.data);\
};\
proto.valueOf=proto.get=function "+className+"_get(){\
return "+(useGetters ? "this.data.get(this.offset)" : "this.data[this.offset]")+
	"};\
proto.set=function "+className+"_set(v){\
return "+(useGetters ? "this.data.set(this.offset,v)" : "this.data[this.offset]=v")+"\
};\
return function construct_"+className+"(a,b,c,d){return new "+className+"(a,d)}"
	    var procedure = new Function("TrivialArray", code)
	    return procedure(CACHED_CONSTRUCTORS[dtype][0])
	  }

	  var code = ["'use strict'"]

	  //Create constructor for view
	  var indices = iota(dimension)
	  var args = indices.map(function(i) { return "i"+i })
	  var index_str = "this.offset+" + indices.map(function(i) {
	        return "this.stride[" + i + "]*i" + i
	      }).join("+")
	  var shapeArg = indices.map(function(i) {
	      return "b"+i
	    }).join(",")
	  var strideArg = indices.map(function(i) {
	      return "c"+i
	    }).join(",")
	  code.push(
	    "function "+className+"(a," + shapeArg + "," + strideArg + ",d){this.data=a",
	      "this.shape=[" + shapeArg + "]",
	      "this.stride=[" + strideArg + "]",
	      "this.offset=d|0}",
	    "var proto="+className+".prototype",
	    "proto.dtype='"+dtype+"'",
	    "proto.dimension="+dimension)

	  //view.size:
	  code.push("Object.defineProperty(proto,'size',{get:function "+className+"_size(){\
return "+indices.map(function(i) { return "this.shape["+i+"]" }).join("*"),
	"}})")

	  //view.order:
	  if(dimension === 1) {
	    code.push("proto.order=[0]")
	  } else {
	    code.push("Object.defineProperty(proto,'order',{get:")
	    if(dimension < 4) {
	      code.push("function "+className+"_order(){")
	      if(dimension === 2) {
	        code.push("return (Math.abs(this.stride[0])>Math.abs(this.stride[1]))?[1,0]:[0,1]}})")
	      } else if(dimension === 3) {
	        code.push(
	"var s0=Math.abs(this.stride[0]),s1=Math.abs(this.stride[1]),s2=Math.abs(this.stride[2]);\
if(s0>s1){\
if(s1>s2){\
return [2,1,0];\
}else if(s0>s2){\
return [1,2,0];\
}else{\
return [1,0,2];\
}\
}else if(s0>s2){\
return [2,0,1];\
}else if(s2>s1){\
return [0,1,2];\
}else{\
return [0,2,1];\
}}})")
	      }
	    } else {
	      code.push("ORDER})")
	    }
	  }

	  //view.set(i0, ..., v):
	  code.push(
	"proto.set=function "+className+"_set("+args.join(",")+",v){")
	  if(useGetters) {
	    code.push("return this.data.set("+index_str+",v)}")
	  } else {
	    code.push("return this.data["+index_str+"]=v}")
	  }

	  //view.get(i0, ...):
	  code.push("proto.get=function "+className+"_get("+args.join(",")+"){")
	  if(useGetters) {
	    code.push("return this.data.get("+index_str+")}")
	  } else {
	    code.push("return this.data["+index_str+"]}")
	  }

	  //view.index:
	  code.push(
	    "proto.index=function "+className+"_index(", args.join(), "){return "+index_str+"}")

	  //view.hi():
	  code.push("proto.hi=function "+className+"_hi("+args.join(",")+"){return new "+className+"(this.data,"+
	    indices.map(function(i) {
	      return ["(typeof i",i,"!=='number'||i",i,"<0)?this.shape[", i, "]:i", i,"|0"].join("")
	    }).join(",")+","+
	    indices.map(function(i) {
	      return "this.stride["+i + "]"
	    }).join(",")+",this.offset)}")

	  //view.lo():
	  var a_vars = indices.map(function(i) { return "a"+i+"=this.shape["+i+"]" })
	  var c_vars = indices.map(function(i) { return "c"+i+"=this.stride["+i+"]" })
	  code.push("proto.lo=function "+className+"_lo("+args.join(",")+"){var b=this.offset,d=0,"+a_vars.join(",")+","+c_vars.join(","))
	  for(var i=0; i<dimension; ++i) {
	    code.push(
	"if(typeof i"+i+"==='number'&&i"+i+">=0){\
d=i"+i+"|0;\
b+=c"+i+"*d;\
a"+i+"-=d}")
	  }
	  code.push("return new "+className+"(this.data,"+
	    indices.map(function(i) {
	      return "a"+i
	    }).join(",")+","+
	    indices.map(function(i) {
	      return "c"+i
	    }).join(",")+",b)}")

	  //view.step():
	  code.push("proto.step=function "+className+"_step("+args.join(",")+"){var "+
	    indices.map(function(i) {
	      return "a"+i+"=this.shape["+i+"]"
	    }).join(",")+","+
	    indices.map(function(i) {
	      return "b"+i+"=this.stride["+i+"]"
	    }).join(",")+",c=this.offset,d=0,ceil=Math.ceil")
	  for(var i=0; i<dimension; ++i) {
	    code.push(
	"if(typeof i"+i+"==='number'){\
d=i"+i+"|0;\
if(d<0){\
c+=b"+i+"*(a"+i+"-1);\
a"+i+"=ceil(-a"+i+"/d)\
}else{\
a"+i+"=ceil(a"+i+"/d)\
}\
b"+i+"*=d\
}")
	  }
	  code.push("return new "+className+"(this.data,"+
	    indices.map(function(i) {
	      return "a" + i
	    }).join(",")+","+
	    indices.map(function(i) {
	      return "b" + i
	    }).join(",")+",c)}")

	  //view.transpose():
	  var tShape = new Array(dimension)
	  var tStride = new Array(dimension)
	  for(var i=0; i<dimension; ++i) {
	    tShape[i] = "a[i"+i+"]"
	    tStride[i] = "b[i"+i+"]"
	  }
	  code.push("proto.transpose=function "+className+"_transpose("+args+"){"+
	    args.map(function(n,idx) { return n + "=(" + n + "===undefined?" + idx + ":" + n + "|0)"}).join(";"),
	    "var a=this.shape,b=this.stride;return new "+className+"(this.data,"+tShape.join(",")+","+tStride.join(",")+",this.offset)}")

	  //view.pick():
	  code.push("proto.pick=function "+className+"_pick("+args+"){var a=[],b=[],c=this.offset")
	  for(var i=0; i<dimension; ++i) {
	    code.push("if(typeof i"+i+"==='number'&&i"+i+">=0){c=(c+this.stride["+i+"]*i"+i+")|0}else{a.push(this.shape["+i+"]);b.push(this.stride["+i+"])}")
	  }
	  code.push("var ctor=CTOR_LIST[a.length+1];return ctor(this.data,a,b,c)}")

	  //Add return statement
	  code.push("return function construct_"+className+"(data,shape,stride,offset){return new "+className+"(data,"+
	    indices.map(function(i) {
	      return "shape["+i+"]"
	    }).join(",")+","+
	    indices.map(function(i) {
	      return "stride["+i+"]"
	    }).join(",")+",offset)}")

	  //Compile procedure
	  var procedure = new Function("CTOR_LIST", "ORDER", code.join("\n"))
	  return procedure(CACHED_CONSTRUCTORS[dtype], order)
	}

	function arrayDType(data) {
	  if(isBuffer(data)) {
	    return "buffer"
	  }
	  if(hasTypedArrays) {
	    switch(Object.prototype.toString.call(data)) {
	      case "[object Float64Array]":
	        return "float64"
	      case "[object Float32Array]":
	        return "float32"
	      case "[object Int8Array]":
	        return "int8"
	      case "[object Int16Array]":
	        return "int16"
	      case "[object Int32Array]":
	        return "int32"
	      case "[object Uint8Array]":
	        return "uint8"
	      case "[object Uint16Array]":
	        return "uint16"
	      case "[object Uint32Array]":
	        return "uint32"
	      case "[object Uint8ClampedArray]":
	        return "uint8_clamped"
	    }
	  }
	  if(Array.isArray(data)) {
	    return "array"
	  }
	  return "generic"
	}

	var CACHED_CONSTRUCTORS = {
	  "float32":[],
	  "float64":[],
	  "int8":[],
	  "int16":[],
	  "int32":[],
	  "uint8":[],
	  "uint16":[],
	  "uint32":[],
	  "array":[],
	  "uint8_clamped":[],
	  "buffer":[],
	  "generic":[]
	}

	;(function() {
	  for(var id in CACHED_CONSTRUCTORS) {
	    CACHED_CONSTRUCTORS[id].push(compileConstructor(id, -1))
	  }
	});

	function wrappedNDArrayCtor(data, shape, stride, offset) {
	  if(data === undefined) {
	    var ctor = CACHED_CONSTRUCTORS.array[0]
	    return ctor([])
	  } else if(typeof data === "number") {
	    data = [data]
	  }
	  if(shape === undefined) {
	    shape = [ data.length ]
	  }
	  var d = shape.length
	  if(stride === undefined) {
	    stride = new Array(d)
	    for(var i=d-1, sz=1; i>=0; --i) {
	      stride[i] = sz
	      sz *= shape[i]
	    }
	  }
	  if(offset === undefined) {
	    offset = 0
	    for(var i=0; i<d; ++i) {
	      if(stride[i] < 0) {
	        offset -= (shape[i]-1)*stride[i]
	      }
	    }
	  }
	  var dtype = arrayDType(data)
	  var ctor_list = CACHED_CONSTRUCTORS[dtype]
	  while(ctor_list.length <= d+1) {
	    ctor_list.push(compileConstructor(dtype, ctor_list.length-1))
	  }
	  var ctor = ctor_list[d+1]
	  return ctor(data, shape, stride, offset)
	}

	module.exports = wrappedNDArrayCtor
	});

	var ndarray$1 = (ndarray && typeof ndarray === 'object' && 'default' in ndarray ? ndarray['default'] : ndarray);

	/**
	 * Returns a copy of the given Coverage object where the
	 * range values which belong to domain areas outside the
	 * given polygon are returned as null (no data).
	 *
	 * @param {Coverage} cov A Coverage object.
	 * @param {Object} polygon A GeoJSON Polygon or MultiPolygon object.
	 * @param {array} [axes=['x','y']] The grid axes corresponding to the polygon coordinates.
	 * @returns {Promise<Coverage>}
	 */
	function maskByPolygon(cov, polygon) {
	  var axes = arguments.length <= 2 || arguments[2] === undefined ? ['x', 'y'] : arguments[2];

	  checkCoverage(cov);

	  if (polygon.type === 'Polygon') {
	    polygon = {
	      type: 'MultiPolygon',
	      coordinates: [polygon.coordinates]
	    };
	  }
	  // prepare polygon coordinates for point-in-big-polygon algorithm
	  var polygons = polygon.coordinates; // .map(poly => poly.map(loop => loop.slice(0, loop.length - 1)))
	  polygons.forEach(function (p) {
	    return ensureClockwisePolygon(p);
	  });

	  var pip = getPointInPolygonsFn(polygons);

	  var _axes = slicedToArray(axes, 2);

	  var X = _axes[0];
	  var Y = _axes[1];


	  return cov.loadDomain().then(function (domain) {
	    var x = domain.axes.get(X).values;
	    var y = domain.axes.get(Y).values;
	    var pnpolyCache = ndarray$1(new Uint8Array(x.length * y.length), [x.length, y.length]);

	    for (var i = 0; i < x.length; i++) {
	      for (var j = 0; j < y.length; j++) {
	        var inside = pip([x[i], y[j]]) >= 0;
	        pnpolyCache.set(i, j, inside);
	      }
	    }

	    var fn = function fn(obj, range) {
	      if (pnpolyCache.get(obj[X] || 0, obj[Y] || 0)) {
	        return range.get(obj);
	      } else {
	        return null;
	      }
	    };

	    var newcov = cov;
	    var _iteratorNormalCompletion = true;
	    var _didIteratorError = false;
	    var _iteratorError = undefined;

	    try {
	      for (var _iterator = cov.parameters.keys()[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
	        var key = _step.value;

	        newcov = mapRange(newcov, key, fn);
	      }
	    } catch (err) {
	      _didIteratorError = true;
	      _iteratorError = err;
	    } finally {
	      try {
	        if (!_iteratorNormalCompletion && _iterator.return) {
	          _iterator.return();
	        }
	      } finally {
	        if (_didIteratorError) {
	          throw _iteratorError;
	        }
	      }
	    }

	    return newcov;
	  });
	}

	exports.minMax = minMax;
	exports.indicesOfNearest = indicesOfNearest;
	exports.indexOfNearest = indexOfNearest;
	exports.DOMAIN = DOMAIN;
	exports.COVERAGE = COVERAGE;
	exports.COVERAGECOLLECTION = COVERAGECOLLECTION;
	exports.COVJSON_DATATYPE_TUPLE = COVJSON_DATATYPE_TUPLE;
	exports.COVJSON_DATATYPE_POLYGON = COVJSON_DATATYPE_POLYGON;
	exports.getLanguageTag = getLanguageTag;
	exports.getLanguageString = getLanguageString;
	exports.stringifyUnit = stringifyUnit;
	exports.minMaxOfRange = minMaxOfRange;
	exports.reduceRange = reduceRange;
	exports.iterateRange = iterateRange;
	exports.getCategory = getCategory;
	exports.isCoverage = isCoverage;
	exports.checkCoverage = checkCoverage;
	exports.isDomain = isDomain;
	exports.checkDomain = checkDomain;
	exports.getReferenceObject = getReferenceObject;
	exports.getHorizontalCRSReferenceObject = getHorizontalCRSReferenceObject;
	exports.isEllipsoidalCRS = isEllipsoidalCRS;
	exports.getProjection = getProjection;
	exports.loadProjection = loadProjection;
	exports.getHorizontalCRSComponents = getHorizontalCRSComponents;
	exports.getHorizontalCRSCoordinateIDs = getHorizontalCRSCoordinateIDs;
	exports.reprojectCoords = reprojectCoords;
	exports.getLongitudeWrapper = getLongitudeWrapper;
	exports.isLongitudeAxis = isLongitudeAxis;
	exports.isISODateAxis = isISODateAxis;
	exports.asTime = asTime;
	exports.normalizeIndexSubsetConstraints = normalizeIndexSubsetConstraints;
	exports.subsetDomainByIndex = subsetDomainByIndex;
	exports.fromDomain = fromDomain;
	exports.fromXndarray = fromXndarray;
	exports.addSubsetFunctions = addSubsetFunctions;
	exports.addLoadRangesFunction = addLoadRangesFunction;
	exports.reproject = reproject;
	exports.subsetByBbox = subsetByBbox;
	exports.subsetByIndex = subsetByIndex;
	exports.subsetByValue = subsetByValue;
	exports.withSimpleDerivedParameter = withSimpleDerivedParameter;
	exports.withParameters = withParameters;
	exports.withCategories = withCategories;
	exports.withDomainType = withDomainType;
	exports.asCovJSONDomainType = asCovJSONDomainType;
	exports.renameAxes = renameAxes;
	exports.mapRange = mapRange;
	exports.withDerivedParameter = withDerivedParameter;
	exports.addCollectionQueryFunction = addCollectionQueryFunction;
	exports.CollectionQuery = CollectionQuery;
	exports.ensureClockwisePolygon = ensureClockwisePolygon;
	exports.getPointInPolygonsFn = getPointInPolygonsFn;
	exports.maskByPolygon = maskByPolygon;

}((this.CovUtils = this.CovUtils || {})));
//# sourceMappingURL=covutils.src.js.map