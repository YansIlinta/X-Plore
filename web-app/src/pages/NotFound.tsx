import { Link } from 'react-router-dom'

export default function NotFound() {
  return (
    <main className="page">
      <div className="empty-state">
        <h2>页面不存在</h2>
        <p>你要找的内容可能已被删除,或者地址输错了。</p>
        <Link className="btn" to="/">
          回首页
        </Link>
      </div>
    </main>
  )
}
