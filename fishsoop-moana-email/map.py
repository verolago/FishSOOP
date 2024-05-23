if sep_map:
    lat_map = []
    lon_map = []
    temp_map = []


def sep_map():
    self.logger.error('In map.py: sep_map')
    try:
        fig1, ax1 = plt.subplots(figsize=(10.75, 7.25),subplot_kw=dict(projection=ccrs.PlateCarree(central_longitude=lon_offset)))
        mpl.rcParams.update({'font.size': 12})
        plt.subplots_adjust(bottom=.25)
        im = Image.open('/home/jjakoboski/Documents/Moana_logos/Moana_Stack_Blue_small.png')
        height = im.size[1]
        width = im.size[0]
        im = np.array(im).astype(np.float) / 255
        gridoff = 0.1
        if not sep_map_box:
            box = [np.nanmin(lon_map%360)-gridoff,np.nanmax(lon_map%360)+gridoff,np.nanmin(lat_map)-gridoff,np.nanmax(lat_map)+gridoff]
        else:
            box = sep_map_box
        ax1.set_extent(box, crs=ccrs.PlateCarree())
        ax1.coastlines(resolution='10m',facecolor='grey')
        land_10m = cfeature.NaturalEarthFeature('physical', 'land', '10m',edgecolor='black',facecolor=cfeature.COLORS['land'])
        ax1.add_feature(land_10m)
        if not (vmin and vmax):
            vmin=np.nanmin(temp_map)
            vmax=np.nanmax(temp_map)
        dot = ax1.scatter(lon_map,lat_map,s=10,c=temp_map,cmap=cmap, vmin=vmin, vmax=vmax, zorder=100)
        cbar = plt.colorbar(dot,label="Temperature ($^\circ$C)")
        plt.title('Bottom or Fishing Temperature \n {} to {}'.format(start_date,end_date))

        gl = ax1.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                          linewidth=1, color='gray', alpha=0.5, linestyle='--')
        gl.xlines = True
       # gl.xlocator = mticker.FixedLocator(np.round(np.linspace(np.nanmin(lon_map)-gridoff,np.nanmax(lon_map)+gridoff,4),1))
        gl.xformatter = LONGITUDE_FORMATTER
        gl.yformatter = LATITUDE_FORMATTER
        gl.xlabel_style = {'size': 15, 'color': 'black'}
        gl.xlabel_style = {'color': 'black'}
        gl.right_labels = gl.top_labels = False
        plt.figimage(im, fig1.bbox.xmax-width-20, fig1.bbox.ymax-height-20,zorder=200)
        plt.savefig(outfiles.format(sn,'all_map'),dpi=75,pad_inches=0,transparent=False)
    except Exception as e:
        print('Skipped overall map, didnt work: {}'.format(e))
