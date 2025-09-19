import type { AppProps } from 'next/app'
import { useEffect, useLayoutEffect } from 'react';
import useSiteTheme from 'hooks/useSiteTheme';
import '../styles/globals.css'
 
export default function MyApp({ Component, pageProps }: AppProps) {
  const theme = useSiteTheme();

  useLayoutEffect(() => {
    if (theme === 'dark') {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [theme]);

  return <Component {...pageProps} />
}