import { Route, Routes } from 'react-router-dom'
import TopNav from './components/TopNav'
import Home from './pages/Home'
import Login from './pages/Login'
import Mine from './pages/Mine'
import NotFound from './pages/NotFound'
import Search from './pages/Search'
import Space from './pages/Space'
import Upload from './pages/Upload'
import Watch from './pages/Watch'
import './App.css'

export default function App() {
  return (
    <div className="shell">
      <TopNav />
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/login" element={<Login />} />
        <Route path="/upload" element={<Upload />} />
        <Route path="/mine" element={<Mine />} />
        <Route path="/watch/:id" element={<Watch />} />
        <Route path="/search" element={<Search />} />
        <Route path="/space/:uid" element={<Space />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
      <footer className="footer">
        X-Plore · 高并发弹幕 + 点播平台演示 ·{' '}
        <a href="https://github.com/YansIlinta" target="_blank" rel="noreferrer">
          GitHub
        </a>
      </footer>
    </div>
  )
}
