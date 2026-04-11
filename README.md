# Client Health Dashboard

A Vue.js application built with TypeScript and Vite, styled with Tailwind CSS. This dashboard provides high-level insights into client health metrics, including realtime merges, failed merges ratio, integration hub errors, and user activity. It supports drilling down into individual client details.

## Features

- High-level overview of all clients
- Drill-down capability for individual clients
- Metrics tracking:
  - Number of realtime merges in the past day
  - Ratio of failed merges
  - Number of errors on integration hub page
  - User activity (active users)

## Tech Stack

- Vue 3 with TypeScript
- Vite for build tooling
- Tailwind CSS for styling

## Getting Started

### Prerequisites

- Node.js (v18 or higher)
- npm

### Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```

### Development

Start the development server:
```bash
npm run dev
```

The app will be available at `http://localhost:5173/`.

### Build

Build for production:
```bash
npm run build
```

Preview the production build:
```bash
npm run preview
```

## Project Structure

- `src/` - Source code
- `public/` - Static assets
- `dist/` - Built files (after running `npm run build`)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and build
5. Submit a pull request
