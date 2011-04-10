from setuptools import setup, find_packages

setup(
      name = 'tumblr_crawler',
      version = '1.0.0',
      packages = find_packages(),
      install_requires = [],
      package_data = {},
      entry_points="""
          [console_scripts]
          tumblr_crawler = tumblr_crawler.tumblr_crawler:main
        """
      )