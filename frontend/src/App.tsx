import React from 'react'
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom'
import MapEditor from './components/MapEditor'
import Dashboard from './components/Dashboard'

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-50">
        <header className="border-b bg-white">
          <div className="mx-auto max-w-6xl px-4 py-4 flex items-center justify-between">
            <Link to="/" className="flex flex-col">
              <div className="text-xl font-bold text-green-700">CPDE</div>
              <div className="text-sm text-gray-600">Crop stress early warning (real satellite + weather)</div>
            </Link>
          </div>
        </header>
        <main className="mx-auto max-w-6xl p-4">
          <Routes>
            <Route path="/" element={<MapEditor />} />
            <Route path="/dashboard/:landId" element={<Dashboard />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
