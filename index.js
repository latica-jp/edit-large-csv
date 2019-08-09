const fs = require('fs')
const Transform = require('stream').Transform
const iconv = require('iconv-lite')
const csv = require('fast-csv')
const parseDate = require('date-fns/parse')

const inputPath = process.argv[2]
const outputPath = process.argv[3]
if (!inputPath || !fs.existsSync(inputPath) || !outputPath) {
  console.error(`Invalid path ${inputPath}`)
  process.exit(0)
}

let csvWs = null
let rowCount = 0
const totalRowCount = 0
const ROW_COUNT_LIMIT = 10000

const addPostFixToPath = (path, rowCount) => {
  const blockNum = Math.floor(rowCount / ROW_COUNT_LIMIT) + 1
  const matches = path.match(/(^.+)(\.csv$)/)
  return `${matches[1]}_${blockNum}${matches[2]}`
}

class StripHeaderIndexTransformer extends Transform {
  constructor(opt) {
    super(opt)
    this.wroteFirstLine = false
  }

  _transform(chunk, enc, cb) {
    if (!this.wroteFirstLine) {
      const line = chunk.toString().replace(/\[\d+\]/g, '')
      this.push(Buffer.from(line))
      this.wroteFirstLine = true
    } else this.push(chunk)
    cb()
  }
}

const createWriteStream = (outputPath, headers) => {
  const ws = csv.format({ headers, quoteColumns: true })
  ws.pipe(new StripHeaderIndexTransformer())
    .pipe(iconv.decodeStream('utf8'))
    .pipe(iconv.encodeStream('Shift-JIS'))
    .pipe(fs.createWriteStream(outputPath))
  return ws
}

const transformRow = row => {
  const tempoId = row['店舗ID']
  if (tempoId) {
    row['店舗ID'] = tempoId.padStart(4, '0')
  }
  const orderDateStr = row['本部発注日']
  if (orderDateStr) {
    const orderDate = parseDate(orderDateStr)
    if (orderDate.getMonth() === 6) row['請求書'] = ''
  }
}

const writeRow = (row, headers, containsTable) => {
  if (!csvWs)
    csvWs = createWriteStream(addPostFixToPath(outputPath, rowCount), headers)
  csvWs.write(row)
  rowCount += 1
  if (rowCount % ROW_COUNT_LIMIT === 0) {
    csvWs.end()
    csvWs = null
  }
}

const writeRowWithTables = (row, headers) => {
  if (!csvWs)
    csvWs = createWriteStream(addPostFixToPath(outputPath, rowCount), headers)
  if (row['レコードの開始行'] === '*') {
    rowCount += 1
    if ((rowCount - 1) % ROW_COUNT_LIMIT === 0) {
      csvWs.end()
      csvWs = createWriteStream(addPostFixToPath(outputPath, rowCount), headers)
    }
  }
  csvWs.write(row)
}

const endWrite = () => {
  if (csvWs) {
    csvWs.end()
  }
}

const readHeaderMap = async () => {
  return new Promise((resolve, reject) => {
    const rs = fs.createReadStream(inputPath)
    const csvRs = rs
      .pipe(iconv.decodeStream('Shift-JIS'))
      .pipe(csv.parse({ headers: false }))
      .on('data', row => {
        // count duplication first because no index required for no duplication
        const duplicationMap = row.reduce((map, column) => {
          const dup = map[column] || 0
          return { ...map, [column]: dup + 1 }
        }, {})
        const indexMap = {}
        const headerMap = row.reduce((map, column) => {
          if (duplicationMap[column] > 1) {
            const index = indexMap[column] || 0
            indexMap[column] = index + 1
            return { ...map, [`${column}[${index}]`]: column }
          } else return { ...map, [column]: column }
        }, {})
        csvRs.destroy()
        // console.log({ duplicationMap, headerMap, indexMap })
        resolve(headerMap)
      })
  })
}

;(async () => {
  const headerMap = await readHeaderMap()
  const headers = Object.keys(headerMap)
  const withTable = headers.includes('レコードの開始行')
  fs.createReadStream(inputPath)
    .pipe(iconv.decodeStream('Shift-JIS'))
    .pipe(csv.parse({ headers: headers, renameHeaders: true }))
    .on('data', row => {
      transformRow(row)
      if (withTable) writeRowWithTables(row, headers)
      else writeRow(row, headers)
    })
    .on('end', () => {
      endWrite()
      console.log({ totalRowCount })
    })
})()
