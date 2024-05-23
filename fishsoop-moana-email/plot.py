import os
import logging
import numpy as np
import cartopy
import boto3
import re
import io
import zipfile
import urllib.request
import cartopy.io.shapereader as shpreader
import cartopy.mpl as cmpl
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import cartopy.feature as cfeature
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from matplotlib.transforms import blended_transform_factory
from matplotlib import gridspec
from PIL import Image
import cmocean as cmo
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

class PlotMangopare(object):
    """
    Mangopare position processing and fishing gear classification.
    Inputs:
        ds: xarray dataset including data to be QC'd
        metadata: pandas dataframe with Mangopare fisher metadata
        These are generated using qc_readers.py, see for defaults.
        filename: original data filename, to be printed on plot
        out_dir: directory where plot will be saved
        logo_file: path and filename for logo, if to be included on plot
        add_map: toggle whether to add a map to plot.  in current format,
        it will look strange if this is set to False
        vmin: minimum temperature value for colorscale (auto calculated
        if not specified)
        vmax: maximum temperature value for colorscale
        lon_offset: use 180 if plotting across 180 meridian (i.e. Pacific)
        cmap: specify matplotlib colormap to use for temperature data
        feature_dir: allows you to specify a directory to look in for
        map coast data, otherwises uses the default below.  If neither are
        available, cartopy looks to it's default online location.
    Outputs:
        savefile: path and filename of saved plot
        time_vals: time range (min and max time values) that the plot covers
    """

    def __init__(self,
                 ds,
                 filename,
                 out_dir=None,
                 logo_file=None,
                 add_map=True,
                 vmin=False,
                 vmax=False,
                 lon_offset=180,
                 feature_dir='s3://fishsoop-qc-tools/cartopy_data/shapefiles/gshhs/',
                 cmap=cmo.cm.thermal,
                 logger=logging):

        self.ds = ds
        self.filename = filename
        self.out_dir = out_dir
        self.logo_file = logo_file
        self.add_map = add_map
        self.vmin = vmin
        self.vmax = vmax
        self.cmap = cmap
        self.feature_dir = feature_dir
        self.lon_offset = lon_offset
        self.logger = logger
        self.savefile = None
        self.feature_dir_default = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'cartopy_data/')
         # Initialize S3 client
        self.s3_client = boto3.client('s3')

    def _calc_statistics(self):
        """
        Create dictionary of deployment stats
        to add to plot
        """
        self.logger.error('In plot.py: _calc_statistics')
        try:
            self.ds['PHASE'] = self.ds['PHASE'].astype(str)
            bdata = self.ds.where(self.ds['PHASE'] == 'D', drop=True)
            # if there are no bottom data, then take means of all
            # measurements in deployment
            if len(bdata['DATETIME']) < 1:
                bdata = self.ds
            tmean = np.nanmean(bdata['TEMPERATURE'].values).round(2)
            dmean = np.nanmean(bdata['DEPTH'].values).round(1)
            tmax = np.nanmax(bdata['TEMPERATURE'].values).round(2)
            tmin = np.nanmin(bdata['TEMPERATURE'].values).round(2)
            stats = {'mean_temp': tmean, 'mean_depth': dmean,
                     "max_temp": tmax, "min_temp": tmin}
            
            #plt.figtext(0.925, 0.11, f'Average fishing depth: {dmean} m \nAverage fishing temperature: {tmean}$^\circ$C \nMin fishing temperature: {tmin}$^\circ$C \nMax fishing temperature: {tmax}$^\circ$C \nData filename: {self.base}',
            #            horizontalalignment='right', verticalalignment='center')
        except Exception as exc:
            self.logger.warning(
                f'Could not calculate statistics for plot {exc}')
            stats = None
        return(stats)

    def _add_logo(self):
        """
        Read logo from file and add to plot
        """
        self.logger.error('In plot.py: _add_logo')
        try:
            """
            im = Image.open(self.logo_file)
            height = im.size[1]
            width = im.size[0]
            im = np.array(im).astype(np.float)
            plt.figimage(im, self.axins.bbox.xmax+width/1.5,
                         self.ax.bbox.ymin-height,zorder=200)
            """
            
            # Initialize the S3 client
            s3_client = boto3.client('s3')
    
            # Specify the bucket name and key (path) of the logo file in S3
            bucket_name = 'fishsoop-qc-tools'
            
            # Download the logo file from S3
            self.logger.error(f'Getting logo {self.logo_file} in {bucket_name}')
            response = s3_client.get_object(Bucket=bucket_name, Key=f'/{self.logo_file}')
            logo_data = response['Body'].read()
        
            im = plt.imread(self.logo_file)
            newax = self.fig.add_axes([0.53, 0.075, 0.1, 0.1], anchor='NE', zorder=10)
            newax.imshow(im)
            newax.axis('off')

        except Exception as exc:
            self.logger.warning(
                f'Could not add logo to plot for {self.filename} due to {exc}')

    def _format_plot(self):
        """
        A bunch of axis formatting things
        """
        self.logger.error('In plot.py: _format_plot')
        try:
            self.ax.set_xlabel('Date (d-m-y:H:M UTC)')
            self.ax.set_ylabel('Depth (m)')

            self.fig.autofmt_xdate(rotation=30, ha='right')
            xfmt = mdates.DateFormatter("%d-%m-%y:%H:%M")
            self.ax.xaxis.set_major_formatter(xfmt)
            self.ax.xaxis.set_minor_formatter(xfmt)

            sn = self.ds.attrs['moana_serial_number']
            if self.ds.attrs['programme_name'] == 'Fish-Soop':
                self.ax.set_title(
                    r'Moana Sensor Measurements,'+f' SN #{sn}')
            else:
                self.ax.set_title(
                    r'Mang$\mathrm{\bar{o}}$pare Sensor Measurements,'+f' SN #{sn}')
        except Exception as exc:
            self.logger.warning(
                f'Plot formatting failed for {self.filename} due to {exc}')

    def _calc_color_range(self):
        """
        Calculates the colors for scatterplot based on
        temperature, and also max and min temp values
        """
        self.logger.error('In plot.py: _calc_color_range')
        colors = np.array(self.ds['TEMPERATURE'].values)
        if not (self.vmin and self.vmax):
            self.vmin = np.nanmin(colors)
            self.vmax = np.nanmax(colors)
            if self.vmin == self.vmax:
                self.vmin = self.vmin-1
                self.vmax = self.vmax+1
        return(colors)

    def _set_axes_limits(self, time_frac=0.1):
        """
        Calculates max and min times, sets x- and y-
        axes limits
        """
        self.logger.error('In plot.py: _set_axes_limits')
        tmax = np.nanmax(self.ds['DATETIME'].values)
        tmin = np.nanmin(self.ds['DATETIME'].values)
        timerange = tmax-tmin
        td = timerange*time_frac
        self.ax.set_xlim([tmin-td, tmax+td])
        self.ax.set_ylim([np.nanmax(self.ds['DEPTH'])+5, 0])
        time_vals = [tmin, tmax]
        return(time_vals)

    def _add_inset_map(self, gridoff=0.1):
        self.logger.error('In plot.py: _add_inset_map')
        self.axins = None
        try:
            transform = blended_transform_factory(self.fig.transFigure, self.ax.transAxes)
            self.axins = inset_axes(self.ax, width="25%", height="60%",
                               bbox_to_anchor=(0.3, 0.2, 1, 1),
                               bbox_transform=transform, loc=8,
                               borderpad=0, axes_class=cmpl.geoaxes.GeoAxes,
                               axes_kwargs=dict(projection=ccrs.PlateCarree(central_longitude=self.lon_offset)))

            lon = self.ds['LONGITUDE'].values
            lat = self.ds['LATITUDE'].values
            temp = self.ds['TEMPERATURE'].values

            box = [np.nanmin(lon % 360)-gridoff, np.nanmax(lon % 360)
                   + gridoff, np.nanmin(lat)-gridoff, np.nanmax(lat)+gridoff]
            self.axins.set_extent(box, crs=ccrs.PlateCarree())

            # Using gshhs
            #bucket_name = 'fishsoop-qc-tools'
            #prefix = 'Coast/osm_land_polygons/'
    
            # List objects in the folder
            #response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            
            # Extract object keys from the response
            #object_keys = [obj['Key'] for obj in response.get('Contents', [])]
            
            #self.logger.error(f'Object keys in {prefix} in {bucket_name}: {object_keys}')

            # Download and load shapefiles
            #for obj in object_keys:
            #    if obj.endswith('.shp'):
            #        self.logger.error(f'obj in loop is: {obj}')
                    # Download the shapefile
                    #try:
                    #    resp = self.s3_client.get_object(Bucket=bucket_name, Key=obj)
                    #    self.logger.error(f'Downloaded object: {bucket_name}/{obj}')
                        
                    #    # Load the shapefile using Cartopy directly from the streaming body
                    #    shp = shpreader.Reader(shp=io.BytesIO(resp['Body'].read()))
                    #    self.logger.error(f'Read object: {bucket_name}/{obj}')
            
                    #    # Process the shapefile as needed
                    #    # For example, add geometries to the map
                    #    self.axins.add_geometries(shp.geometries(), ccrs.PlateCarree(),
                    #                               edgecolor='black', facecolor='none')
            
                    #except Exception as e:
                    #    self.logger.error(f"Error processing shapefile {obj}: {e}")

            
            # Using gshhs
            #shpfile = cartopy.io.shapereader.gshhs('h')
            #shp = cartopy.io.shapereader.Reader(shpfile)
            
            #self.axins.add_geometries(shp.geometries(), ccrs.PlateCarree(
            #    ), edgecolor='black', facecolor=cfeature.COLORS['land'])

            self.axins.scatter(lon+self.lon_offset, lat, s=10, c=temp,
                          cmap=self.cmap, vmin=self.vmin, vmax=self.vmax, zorder=100)
            gl = self.axins.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                                 linewidth=1, color='gray', alpha=0.5, linestyle='--')
            gl.xlines = True
            gl.xlocator = mticker.FixedLocator(np.round(np.linspace(
                np.nanmin(lon)-gridoff, np.nanmax(lon)+gridoff, 4), 1))
            gl.xformatter = LONGITUDE_FORMATTER
            gl.yformatter = LATITUDE_FORMATTER
            gl.xlabel_style = {'size': 15, 'color': 'black'}
            gl.xlabel_style = {'color': 'black'}
            gl.right_labels = gl.top_labels = False
        except Exception as exc:
            self.logger.warning(
                f'Skipped map for {self.filename} because {exc}')
            if self.axins:
                self.axins.remove()

    def _set_outfile(self):
        self.logger.error('In plot.py: _set_outfile')
        try:
            in_dir, self.base = os.path.split(self.filename)
            
            # Determine Moana serial number from the filename
            moana_serial_number = self.filename.split('/')[1]
            moana_serial_number_match = re.search(r'MOANA_(\d{4})_', self.filename)
            moana_serial_number = moana_serial_number_match.group(1)
            
            out_dir = f'/{moana_serial_number}'
            
            filebase = os.path.splitext(self.base)[0]
            if not self.out_dir:
                self.out_dir = out_dir
            self.savefile = os.path.join(self.out_dir, f'{filebase}_plot.png')
        except Exception as exc:
            self.logger.warning(
                f'Could not calculate name to save plot because {exc}')

    def _create_plot(self, colors, fontsize=12, figsize=(12, 5.5)):
        self.logger.error('In plot.py: _create_plot')
        try:
            mpl.rcParams.update({'font.size': 12})
            self.fig = plt.figure(figsize=figsize)
            gs = gridspec.GridSpec(nrows=1, ncols=2, width_ratios=[2, 1])
            self.ax = self.fig.add_subplot(gs[0])

            plt.subplots_adjust(bottom=.25)

            dot_c = self.ax.scatter(self.ds['DATETIME'].values, self.ds['DEPTH'].values,
                                    c=colors, cmap=self.cmap, vmin=self.vmin, vmax=self.vmax)
            self.fig.colorbar(
                dot_c, label="Temperature ($^\circ$C)", ax=self.ax)

            self.time_vals = self._set_axes_limits()
            self._format_plot()

            self._add_inset_map()

            self._calc_statistics()
            self.logger.error('In plot.py: back to _create_plot after _calc_statistics')

            #if self.logo_file:
            #    self._add_logo()

            #plt.savefig(self.savefile, dpi=75, bbox_inches='tight', pad_inches=0.25, transparent=False)
            
            # Convert plot image to bytes buffer
            buffer = io.BytesIO()
            plt.savefig(buffer, dpi=75, bbox_inches='tight', pad_inches=0.25, transparent=False)
            buffer.seek(0)
            self.logger.error('In plot.py: _create_plot after the buffer image to bytes')
            
            # Upload plot image to S3
            png_filename = os.path.splitext(os.path.basename(self.filename))[0] + '.png'
            self.logger.error(f'Saving the plot {png_filename} in {self.savefile}')
            
            # Remove slash from self.savefile if it exists
            if self.savefile.startswith('/'):
                self.savefile = self.savefile[1:]

            self.s3_client.put_object(
                Body=buffer,
                Bucket='fishsoop-email', 
                Key=f'{self.savefile}'
            )
            
            # Remove folder name from plot filename
            self.savefile = os.path.basename(self.savefile)
            
            self.logger.error(f'in plot.py: cerate_plot: the self.savefile is {self.savefile}')

        except Exception as exc:
            self.logger.error(
                f'Could not generate plot for {self.filename} due to {exc}')

    def _set_cartopy_config(self):
        """
        Use local coastline and land files if available to avoid downloads
        when possible/necessary
        """
        self.logger.error('In plot.py: _set_cartopy_config')
        if not self.feature_dir:
            self.feature_dir = self.feature_dir_default
        cartopy.config['pre_existing_data_dir'] = self.feature_dir

    def run(self):
        self.logger.error('In plot.py: run')
        try:
            self._set_cartopy_config()
            self._set_outfile()
            colors = self._calc_color_range()
            self._create_plot(colors)
            plt.close(self.fig)
            #if os.path.isfile(self.savefile):
            return(self.savefile, self.time_vals)
            #else:
            #    return(None, None)
        except Exception as exc:
            self.logger.error(f'Did not save {self.savefile} due to {exc}')
            return(None, None)
