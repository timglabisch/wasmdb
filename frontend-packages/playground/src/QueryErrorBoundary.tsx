import { Component, type ReactNode } from 'react';

interface BoundaryState { error: string | null }

export class QueryErrorBoundary extends Component<{ children: ReactNode }, BoundaryState> {
  state: BoundaryState = { error: null };
  static getDerivedStateFromError(error: unknown): BoundaryState {
    return { error: error instanceof Error ? error.message : String(error) };
  }
  componentDidUpdate(prev: { children: ReactNode }) {
    if (prev.children !== this.props.children && this.state.error) {
      this.setState({ error: null });
    }
  }
  render() {
    if (this.state.error) {
      return <span className="query-error" title={this.state.error}>error: {this.state.error}</span>;
    }
    return this.props.children;
  }
}
