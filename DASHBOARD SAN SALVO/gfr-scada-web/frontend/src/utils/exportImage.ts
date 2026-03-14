function copyComputedStyles(source: Element, target: Element) {
  const sourceStyle = window.getComputedStyle(source)
  const targetStyle = (target as HTMLElement).style

  for (const property of Array.from(sourceStyle)) {
    targetStyle.setProperty(
      property,
      sourceStyle.getPropertyValue(property),
      sourceStyle.getPropertyPriority(property)
    )
  }
}

function cloneNodeWithStyles(node: Node): Node {
  if (node.nodeType === Node.TEXT_NODE) {
    return document.createTextNode(node.textContent || '')
  }

  if (!(node instanceof Element)) {
    return node.cloneNode(false)
  }

  const clone = node.cloneNode(false) as Element
  copyComputedStyles(node, clone)

  if (clone instanceof SVGElement) {
    clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg')
  }

  for (const child of Array.from(node.childNodes)) {
    clone.appendChild(cloneNodeWithStyles(child))
  }

  return clone
}

function buildSvgDataUrl(element: HTMLElement, width: number, height: number) {
  const clonedNode = cloneNodeWithStyles(element) as HTMLElement
  clonedNode.setAttribute('xmlns', 'http://www.w3.org/1999/xhtml')

  const serializer = new XMLSerializer()
  const xhtml = serializer.serializeToString(clonedNode)
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
      <foreignObject width="100%" height="100%">${xhtml}</foreignObject>
    </svg>
  `

  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`
}

export async function exportElementAsPng(element: HTMLElement, filename: string) {
  const rect = element.getBoundingClientRect()
  const width = Math.max(1, Math.ceil(rect.width))
  const height = Math.max(1, Math.ceil(rect.height))
  const dataUrl = buildSvgDataUrl(element, width, height)

  await new Promise<void>((resolve, reject) => {
    const image = new Image()
    image.onload = () => {
      const canvas = document.createElement('canvas')
      canvas.width = width * 2
      canvas.height = height * 2
      const context = canvas.getContext('2d')
      if (!context) {
        reject(new Error('Canvas context not available'))
        return
      }

      context.scale(2, 2)
      context.fillStyle = '#ffffff'
      context.fillRect(0, 0, width, height)
      context.drawImage(image, 0, 0, width, height)

      const pngUrl = canvas.toDataURL('image/png')
      const link = document.createElement('a')
      link.href = pngUrl
      link.download = filename
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      resolve()
    }
    image.onerror = () => reject(new Error('Unable to render image export'))
    image.src = dataUrl
  })
}
