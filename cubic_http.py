#!/usr/bin/env python3

# Very beta version!

import html
import logging
import os
import posixpath
import socketserver
import sys
import urllib.parse
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler
from config import block_size

from cubic_fuse import CubicFS, FuseOSError


class CubicHTTPRequestHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        f = self.send_head()
        for blk in f:
            self.wfile.write(blk)

    def do_HEAD(self):
        f = self.send_head()
        for blk in f:
            break

    def translate_path(self, path):
        path = path.split('?', 1)[0]
        path = path.split('#', 1)[0]
        trailing_slash = path.rstrip().endswith('/')
        try:
            path = urllib.parse.unquote(path, errors='surrogatepass')
        except UnicodeDecodeError:
            path = urllib.parse.unquote(path)
        path = posixpath.normpath(path)
        words = path.split('/')
        words = filter(None, words)
        path = '/'
        for word in words:
            if os.path.dirname(word) or word in (os.curdir, os.pardir):
                # Ignore components that are not a simple file/directory name
                continue
            path = os.path.join(path, word)
        if trailing_slash and not path.endswith('/'):
            path += '/'
        return path

    def send_head(self):
        self.server.fs: CubicFS
        self.directory = '/'
        path = self.translate_path(self.path)
        logging.info('Requesting %s', path)
        path_no_trailing = path
        if path_no_trailing.endswith('/') and path_no_trailing != '/':
            path_no_trailing = path_no_trailing[:-1]
        try:
            list = self.server.fs.readdir(path_no_trailing)
            if '.' in list:
                list.remove('.')
            if '..' in list:
                list.remove('..')
        except FuseOSError:  # file
            try:
                attr = self.server.fs.getattr(path)
            except FuseOSError:
                self.send_error(HTTPStatus.NOT_FOUND, "File not found")
                return
            ctype = self.guess_type(path)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-type", ctype)
            self.send_header("Content-Length", str(attr['st_size']))
            self.send_header("Last-Modified", self.date_time_string(attr['st_mtime']))
            self.end_headers()
            start = 0
            size = attr['st_size']
            yield b''
            while start < size:
                yield self.server.fs.read(path, min(block_size, size - start), start)
                start += block_size
        else:  # dir
            parts = urllib.parse.urlsplit(self.path)
            if not parts.path.endswith('/'):
                self.send_response(HTTPStatus.MOVED_PERMANENTLY)
                new_parts = (parts[0], parts[1], parts[2] + '/',
                             parts[3], parts[4])
                new_url = urllib.parse.urlunsplit(new_parts)
                self.send_header("Location", new_url)
                self.end_headers()
                return
            else:
                list.sort(key=lambda a: a.lower())
                r = []
                try:
                    displaypath = urllib.parse.unquote(self.path,
                                                       errors='surrogatepass')
                except UnicodeDecodeError:
                    displaypath = urllib.parse.unquote(path)
                displaypath = html.escape(displaypath, quote=False)
                enc = sys.getfilesystemencoding()
                title = 'Directory listing for %s' % displaypath
                r.append('<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" '
                         '"http://www.w3.org/TR/html4/strict.dtd">')
                r.append('<html>\n<head>')
                r.append('<meta http-equiv="Content-Type" '
                         'content="text/html; charset=%s">' % enc)
                r.append('<title>%s</title>\n</head>' % title)
                r.append('<body>\n<h1>%s</h1>' % title)
                r.append('<hr>\n<ul>')
                for name in list:
                    displayname = linkname = name
                    try:
                        self.server.fs.readdir(os.path.join(self.path, name))
                    except:
                        pass
                    else:
                        displayname = name + "/"
                        linkname = name + "/"
                    r.append('<li><a href="%s">%s</a></li>'
                             % (urllib.parse.quote(linkname,
                                                   errors='surrogatepass'),
                                html.escape(displayname, quote=False)))
                r.append('</ul>\n<hr>\n</body>\n</html>\n')
                encoded = '\n'.join(r).encode(enc, 'surrogateescape')
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-type", "text/html; charset=%s" % enc)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                yield encoded


if __name__ == '__main__':
    if len(sys.argv) >= 4:
        key = sys.argv[3]
    else:
        key = None

    logging.basicConfig(level=logging.INFO)

    LISTEN = "127.0.0.1", 8000

    with socketserver.TCPServer(LISTEN, CubicHTTPRequestHandler) as httpd:
        httpd.fs = CubicFS(sys.argv[1], sys.argv[2], key)
        logging.info('Listening at %s', LISTEN)
        httpd.serve_forever()
