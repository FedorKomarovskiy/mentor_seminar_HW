import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from typing import Tuple, Optional, Dict, Any
import logging
from datetime import datetime, date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

DEFAULT_FIGSIZE = (12, 6)
DEFAULT_BINS = 20
DEFAULT_DPI = 300


def create_time_series_revenue_chart(
    df: pd.DataFrame,
    date_column: str = 'dt',
    revenue_column: str = 'revenue',
    figsize: Tuple[int, int] = DEFAULT_FIGSIZE,
    title: str = "Daily Revenue Time Series",
    save_path: Optional[str] = None
) -> plt.Figure:
    """
    Generate a professional time-series chart for revenue data.
    
    Args:
        df: DataFrame containing date and revenue data
        date_column: Name of the date column
        revenue_column: Name of the revenue column
        figsize: Figure size as (width, height)
        title: Chart title
        save_path: Optional path to save the chart
        
    Returns:
        Matplotlib figure object
    """
    required_columns = [date_column, revenue_column]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    try:
        fig, ax = plt.subplots(figsize=figsize)
        
        dates = pd.to_datetime(df[date_column])
        revenue = df[revenue_column]
        
        ax.plot(dates, revenue, 
                marker='o', 
                linewidth=2, 
                markersize=4,
                color='#2E86AB',
                markerfacecolor='#A23B72',
                markeredgecolor='white',
                markeredgewidth=1)
        
        ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Date', fontsize=12, fontweight='bold')
        ax.set_ylabel('Revenue ($)', fontsize=12, fontweight='bold')
        
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates)//10)))
        
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        ax.grid(True, alpha=0.3, linestyle='--')
        
        avg_revenue = revenue.mean()
        max_revenue = revenue.max()
        min_revenue = revenue.min()
        
        stats_text = f'Avg: ${avg_revenue:,.0f}\\nMax: ${max_revenue:,.0f}\\nMin: ${min_revenue:,.0f}'
        ax.text(0.02, 0.98, stats_text, 
                transform=ax.transAxes, 
                verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                fontsize=10)
        
        plt.tight_layout()
        
        if save_path:
            fig.savefig(save_path, dpi=DEFAULT_DPI, bbox_inches='tight')
            logger.info(f"Chart saved to {save_path}")
        
        logger.info(f"Time-series revenue chart created with {len(df)} data points")
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create time-series revenue chart: {e}")
        raise RuntimeError(f"Could not create time-series revenue chart: {e}")


def create_payment_coverage_histogram(
    df: pd.DataFrame,
    coverage_column: str = 'payment_coverage',
    bins: int = DEFAULT_BINS,
    figsize: Tuple[int, int] = (10, 6),
    title: str = "Payment Coverage Distribution",
    save_path: Optional[str] = None
) -> plt.Figure:
    """
    Generate a professional histogram for payment coverage distribution.
    
    Args:
        df: DataFrame containing payment coverage data
        coverage_column: Name of the payment coverage column
        bins: Number of histogram bins
        figsize: Figure size as (width, height)
        title: Chart title
        save_path: Optional path to save the chart
        
    Returns:
        Matplotlib figure object
    """
    if coverage_column not in df.columns:
        raise ValueError(f"Column '{coverage_column}' not found in DataFrame")
    
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    try:
        fig, ax = plt.subplots(figsize=figsize)
        
        coverage_data = df[coverage_column].dropna()
        
        if coverage_data.empty:
            raise ValueError(f"No valid data in column '{coverage_column}'")
        
        n, bins_edges, patches = ax.hist(coverage_data, 
                                        bins=bins,
                                        alpha=0.7,
                                        color='#F18F01',
                                        edgecolor='black',
                                        linewidth=0.5)
        
        for i, patch in enumerate(patches):
            bin_center = (bins_edges[i] + bins_edges[i+1]) / 2
            if bin_center >= 0.9:
                patch.set_facecolor('#2E8B57')
            elif bin_center >= 0.7:
                patch.set_facecolor('#FFD700')
            else:
                patch.set_facecolor('#DC143C')
        
        ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Payment Coverage Ratio', fontsize=12, fontweight='bold')
        ax.set_ylabel('Frequency (Number of Days)', fontsize=12, fontweight='bold')
        
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0%}'))
        
        percentiles = [0.25, 0.5, 0.75]
        colors = ['red', 'orange', 'green']
        
        for percentile, color in zip(percentiles, colors):
            value = coverage_data.quantile(percentile)
            ax.axvline(value, color=color, linestyle='--', alpha=0.7, linewidth=2)
            ax.text(value, ax.get_ylim()[1] * 0.9, 
                   f'{percentile*100:.0f}th: {value:.1%}',
                   rotation=90, ha='right', va='top',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        ax.grid(True, alpha=0.3, linestyle='--', axis='y')
        
        mean_coverage = coverage_data.mean()
        median_coverage = coverage_data.median()
        std_coverage = coverage_data.std()
        
        stats_text = f'Mean: {mean_coverage:.1%}\\nMedian: {median_coverage:.1%}\\nStd: {std_coverage:.1%}'
        ax.text(0.98, 0.98, stats_text, 
                transform=ax.transAxes, 
                verticalalignment='top',
                horizontalalignment='right',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
                fontsize=10)
        
        legend_elements = [
            plt.Rectangle((0,0),1,1, facecolor='#2E8B57', label='High Coverage (≥90%)'),
            plt.Rectangle((0,0),1,1, facecolor='#FFD700', label='Medium Coverage (70-90%)'),
            plt.Rectangle((0,0),1,1, facecolor='#DC143C', label='Low Coverage (<70%)')
        ]
        ax.legend(handles=legend_elements, loc='upper left')
        
        plt.tight_layout()
        
        if save_path:
            fig.savefig(save_path, dpi=DEFAULT_DPI, bbox_inches='tight')
            logger.info(f"Chart saved to {save_path}")
        
        logger.info(f"Payment coverage histogram created with {len(coverage_data)} data points")
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create payment coverage histogram: {e}")
        raise RuntimeError(f"Could not create payment coverage histogram: {e}")


def create_combined_analytics_dashboard(
    df: pd.DataFrame,
    figsize: Tuple[int, int] = (16, 12),
    title: str = "Analytics Dashboard",
    save_path: Optional[str] = None
) -> plt.Figure:
    """
    Create a comprehensive analytics dashboard with multiple visualizations.
    
    Args:
        df: DataFrame containing all analytics data
        figsize: Figure size as (width, height)
        title: Dashboard title
        save_path: Optional path to save the dashboard
        
    Returns:
        Matplotlib figure object
    """
    required_columns = ['dt', 'revenue', 'orders_cnt', 'payments_cnt', 'paid_amount', 'payment_coverage']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    try:
        fig, axes = plt.subplots(2, 3, figsize=figsize)
        fig.suptitle(title, fontsize=20, fontweight='bold', y=0.98)
        
        dates = pd.to_datetime(df['dt'])
        
        axes[0, 0].plot(dates, df['revenue'], marker='o', linewidth=2, color='#2E86AB')
        axes[0, 0].set_title('Daily Revenue', fontweight='bold')
        axes[0, 0].set_ylabel('Revenue ($)')
        axes[0, 0].yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        axes[0, 0].grid(True, alpha=0.3)
        axes[0, 0].tick_params(axis='x', rotation=45)
        
        axes[0, 1].plot(dates, df['orders_cnt'], marker='s', linewidth=2, color='#A23B72')
        axes[0, 1].set_title('Daily Orders Count', fontweight='bold')
        axes[0, 1].set_ylabel('Orders Count')
        axes[0, 1].grid(True, alpha=0.3)
        axes[0, 1].tick_params(axis='x', rotation=45)
        
        axes[0, 2].plot(dates, df['payment_coverage'], marker='^', linewidth=2, color='#F18F01')
        axes[0, 2].set_title('Payment Coverage', fontweight='bold')
        axes[0, 2].set_ylabel('Coverage Ratio')
        axes[0, 2].yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0%}'))
        axes[0, 2].grid(True, alpha=0.3)
        axes[0, 2].tick_params(axis='x', rotation=45)
        
        axes[1, 0].scatter(df['revenue'], df['paid_amount'], alpha=0.6, color='#C73E1D')
        axes[1, 0].set_title('Revenue vs Paid Amount', fontweight='bold')
        axes[1, 0].set_xlabel('Revenue ($)')
        axes[1, 0].set_ylabel('Paid Amount ($)')
        axes[1, 0].xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        axes[1, 0].yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        max_val = max(df['revenue'].max(), df['paid_amount'].max())
        axes[1, 0].plot([0, max_val], [0, max_val], 'k--', alpha=0.5, label='Perfect Coverage')
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)
        
        axes[1, 1].hist(df['payment_coverage'], bins=15, alpha=0.7, color='#3F88C5', edgecolor='black')
        axes[1, 1].set_title('Payment Coverage Distribution', fontweight='bold')
        axes[1, 1].set_xlabel('Coverage Ratio')
        axes[1, 1].set_ylabel('Frequency')
        axes[1, 1].xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.0%}'))
        axes[1, 1].grid(True, alpha=0.3, axis='y')
        
        axes[1, 2].axis('off')
        
        stats_data = [
            ['Metric', 'Value'],
            ['Total Days', f"{len(df)}"],
            ['Avg Daily Revenue', f"${df['revenue'].mean():,.0f}"],
            ['Total Revenue', f"${df['revenue'].sum():,.0f}"],
            ['Avg Daily Orders', f"{df['orders_cnt'].mean():.1f}"],
            ['Total Orders', f"{df['orders_cnt'].sum():,}"],
            ['Avg Coverage', f"{df['payment_coverage'].mean():.1%}"],
            ['Days w/ Full Coverage', f"{(df['payment_coverage'] >= 1.0).sum()}"]
        ]
        
        table = axes[1, 2].table(cellText=stats_data[1:], 
                                colLabels=stats_data[0],
                                cellLoc='center',
                                loc='center',
                                colWidths=[0.6, 0.4])
        
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2)
        
        for i in range(len(stats_data)):
            for j in range(len(stats_data[0])):
                cell = table[(i, j)]
                if i == 0:
                    cell.set_facecolor('#4472C4')
                    cell.set_text_props(weight='bold', color='white')
                else:
                    cell.set_facecolor('#F2F2F2' if i % 2 == 0 else 'white')
        
        axes[1, 2].set_title('Summary Statistics', fontweight='bold')
        
        plt.tight_layout()
        plt.subplots_adjust(top=0.93)
        
        if save_path:
            fig.savefig(save_path, dpi=DEFAULT_DPI, bbox_inches='tight')
            logger.info(f"Dashboard saved to {save_path}")
        
        logger.info(f"Analytics dashboard created with {len(df)} data points")
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create analytics dashboard: {e}")
        raise RuntimeError(f"Could not create analytics dashboard: {e}")


def save_all_charts(
    df: pd.DataFrame,
    output_dir: str = "charts",
    file_format: str = "png"
) -> Dict[str, str]:
    """
    Generate and save all chart types to specified directory.
    
    Args:
        df: DataFrame containing analytics data
        output_dir: Directory to save charts
        file_format: File format for saved charts (png, jpg, pdf, svg)
        
    Returns:
        Dictionary mapping chart names to file paths
    """
    import os
    
    try:
        os.makedirs(output_dir, exist_ok=True)
        
        saved_files = {}
        
        revenue_path = os.path.join(output_dir, f"revenue_timeseries.{file_format}")
        fig1 = create_time_series_revenue_chart(df, save_path=revenue_path)
        plt.close(fig1)
        saved_files['revenue_timeseries'] = revenue_path
        
        coverage_path = os.path.join(output_dir, f"payment_coverage_histogram.{file_format}")
        fig2 = create_payment_coverage_histogram(df, save_path=coverage_path)
        plt.close(fig2)
        saved_files['payment_coverage_histogram'] = coverage_path
        
        dashboard_path = os.path.join(output_dir, f"analytics_dashboard.{file_format}")
        fig3 = create_combined_analytics_dashboard(df, save_path=dashboard_path)
        plt.close(fig3)
        saved_files['analytics_dashboard'] = dashboard_path
        
        logger.info(f"All charts saved to {output_dir}")
        return saved_files
        
    except Exception as e:
        logger.error(f"Failed to save charts: {e}")
        raise RuntimeError(f"Could not save charts: {e}")


def display_chart_summary(df: pd.DataFrame) -> None:
    """
    Display a summary of the data that will be used for charting.
    
    Args:
        df: DataFrame containing analytics data
    """
    print("📊 Chart Data Summary")
    print("=" * 50)
    print(f"Data points: {len(df)}")
    print(f"Date range: {df['dt'].min()} to {df['dt'].max()}")
    print(f"Revenue range: ${df['revenue'].min():,.2f} - ${df['revenue'].max():,.2f}")
    print(f"Coverage range: {df['payment_coverage'].min():.1%} - {df['payment_coverage'].max():.1%}")
    print(f"Average coverage: {df['payment_coverage'].mean():.1%}")
    print("=" * 50)